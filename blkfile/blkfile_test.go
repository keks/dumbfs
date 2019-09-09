package rwablk

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type testReadWriterAt struct {
	buf []byte
}

func (rwa *testReadWriterAt) ReadAt(buf []byte, off int64) (int, error) {
	if off != int64(int(off)) {
		return 0, io.EOF
	}

	if int(off) >= len(rwa.buf) {
		return 0, io.EOF
	}

	max := len(rwa.buf) - int(off)
	var err error
	if max < len(buf) {
		buf = buf[:max]
		err = io.EOF
	}

	copy(buf, rwa.buf[int(off):])

	return len(buf), err
}

func (rwa *testReadWriterAt) WriteAt(data []byte, off int64) (int, error) {
	if off != int64(int(off)) {
		return 0, io.EOF
	}

	if int(off)+len(data) > len(rwa.buf) {
		rwa.buf = append(rwa.buf, make([]byte, int(off)+len(data)-len(rwa.buf))...)
	}

	copy(rwa.buf[int(off):], data)

	return len(data), nil
}

type op interface {
	Do(*testing.T, *block)
}

type blkWriteOp struct {
	data []byte
	off  int64

	expN   int
	expErr string
}

func (op blkWriteOp) Do(t *testing.T, blk *block) {
	r := require.New(t)
	n, err := blk.WriteAt(op.data, op.off)

	r.Equal(op.expN, n)
	if op.expErr == "" {
		r.NoError(err)
	} else {
		r.EqualError(err, op.expErr)
	}
}

type blkReadOp struct {
	off int64
	readlen int

	exp    []byte
	expN   int
	expErr string
}

func (op blkReadOp) Do(t *testing.T, blk *block) {
	r := require.New(t)
	if op.readlen == 0 {
		op.readlen = len(op.exp)
	}

	buf := make([]byte, op.readlen)
	n, err := blk.ReadAt(buf, op.off)

	if op.expErr == "" {
		r.NoError(err)
	} else {
		r.EqualError(err, op.expErr)
	}
	r.Equal(op.expN, n)
	r.True(bytes.Equal(buf[:op.expN], op.exp))
}

func TestBlock(t *testing.T) {
	type testcase struct {
		name string
		size int
		ops  []op
	}

	mktest := func(tc testcase) func(*testing.T) {
		return func(t *testing.T) {
			blk := &block{
				lower: &testReadWriterAt{[]byte{}},
				size:  tc.size,
			}

			for _, op := range tc.ops {
				op.Do(t, blk)
			}

			f, err := ioutil.TempFile("", "TestBlock-*")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			blk = &block{
				lower: f,
				size:  tc.size,
			}

			for _, op := range tc.ops {
				op.Do(t, blk)
			}
		}
	}

	var tcs = []testcase{
		{
			name: "set then get",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data: []byte("test"),
					off:  0,
					expN: 4,
				},
				blkReadOp{
					off:  0,
					exp:  []byte("test"),
					expN: 4,
				},
			},
		},
		{
			name: "set long then get short",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data: []byte("testtest"),
					off:  0,
					expN: 8,
				},
				blkReadOp{
					off:  0,
					exp:  []byte("test"),
					expN: 4,
				},
			},
		},
		{
			name: "write over block end",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data:   []byte("test"),
					off:    (1 << 10) - 2,
					expN:   2,
					expErr: "EOF",
				},
				blkReadOp{
					off:    (1 << 10) - 2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("te"),
					readlen: 4,
				},
			},
		},
		{
			name: "write after block end",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data:   []byte("test"),
					off:    (1 << 10) + 2,
					expN:   0,
					expErr: "EOF",
				},
			},
		},
		{
			name: "read over inner end",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data:   []byte("test"),
					off:    0,
					expN:   4,
				},
				blkReadOp{
					off:    2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("st"),
					readlen: 4,
				},
			},
		},
		{
			name: "read over block end",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data:   bytes.Repeat([]byte("test"), 1<<8),
					off:    0,
					expN:   1 << 10,
				},
				blkReadOp{
					off:    (1 << 10) - 2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("st"),
					readlen: 4,
				},
			},
		},
		{
			name: "read after block end",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data:   bytes.Repeat([]byte("test"), 1<<8),
					off:    0,
					expN:   1 << 10,
				},
				blkReadOp{
					off:    (1 << 10) + 2,
					expN:   0,
					expErr: "EOF",
					exp:    []byte(""),
					readlen: 4,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, mktest(tc))
	}
}
