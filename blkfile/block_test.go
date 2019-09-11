package rwablk

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/keks/dumbfs"

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
	Do(*testing.T, dumbfs.ReadWriterAt)
}

type blkOpenOp struct {
	blk *block
	id dumbfs.BlockID

	expErr string
}

func (op blkOpenOp) Do(t *testing.T, rwa dumbfs.ReadWriterAt) {
	blk, err := openBlock(rwa, int64(op.id))
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	*op.blk = *blk
}

type blkNewOp struct {
	blk *block
	size int
	id dumbfs.BlockID

	expErr string
}

func (op blkNewOp) Do(t *testing.T, rwa dumbfs.ReadWriterAt) {
	blk, err := newBlock(rwa, int64(op.id), op.size)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	*op.blk = *blk
}




type blkWriteOp struct {
	blk *block
	data []byte
	off  int64

	// set these if blk == nil
	blkOff int64
	blkSize int

	expN   int
	expErr string
}

func (op blkWriteOp) Do(t *testing.T, rwa dumbfs.ReadWriterAt) {
	r := require.New(t)

	t.Log("writeOp, op.blk:", op.blk)

	if op.blk == nil {
		op.blk = &block{
			lower: rwa,
			off:   op.blkOff,
			size:  op.blkSize,
		}
	}

	t.Log("writeOp, op.blk:", op.blk)

	n, err := op.blk.WriteAt(op.data, op.off)

	t.Logf("writeOp, n: %d, err: %v", n, err)

	r.Equal(op.expN, n)
	if op.expErr == "" {
		r.NoError(err)
	} else {
		r.EqualError(err, op.expErr)
	}
}

type blkReadOp struct {
	blk  *block
	off int64
	readlen int

	// set these if blk == nil
	blkOff int64
	blkSize int

	exp    []byte
	expN   int
	expErr string
}

func (op blkReadOp) Do(t *testing.T, rwa dumbfs.ReadWriterAt) {
	r := require.New(t)
	if op.readlen == 0 {
		op.readlen = len(op.exp)
	}

	t.Log("readOp, op.blk:", op.blk)

	if op.blk == nil {
		op.blk = &block{
			lower: rwa,
			off:   op.blkOff,
			size:  op.blkSize,
		}
	}

	t.Log("readOp, op.blk:", op.blk)
	t.Logf("readOp, op.blk.lower:%#v", op.blk.lower)

	buf := make([]byte, op.readlen)
	n, err := op.blk.ReadAt(buf, op.off)

	t.Logf("readOp, n: %d, err: %v", n, err)

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
			// test with memory ReadWriterAt
			rwa := &testReadWriterAt{[]byte{}}
			for _, op := range tc.ops {
				op.Do(t, rwa)
			}

			// test with os.File as ReadWriterAt
			f, err := ioutil.TempFile("", "TestBlock-*")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			for _, op := range tc.ops {
				op.Do(t, f)
			}
		}
	}

	var blk, blk2 block

	var tcs = []testcase{
		{
			name: "set then get",
			size: 1 << 10,
			ops: []op{
				blkWriteOp{
					data: []byte("test"),
					off:  0,

					expN: 4,

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:  0,
					exp:  []byte("test"),
					expN: 4,

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:  0,
					exp:  []byte("test"),
					expN: 4,

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:    (1 << 10) - 2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("te"),
					readlen: 4,

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:    2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("st"),
					readlen: 4,

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:    (1 << 10) - 2,
					expN:   2,
					expErr: "EOF",
					exp:    []byte("st"),
					readlen: 4,

					blkSize: 1 << 10,
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

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:    (1 << 10) + 2,
					expN:   0,
					expErr: "EOF",
					exp:    []byte(""),
					readlen: 4,

					blkSize: 1 << 10,
				},
			},
		},
		{
			name: "new, set then get",
			ops: []op{
				blkNewOp{
					blk: &blk,

					size: 1 << 10,
				},
				blkWriteOp{
					blk: &blk,

					data: []byte("test"),
					off:  0,

					expN: 4,
				},
				blkReadOp{
					blk: &blk,

					off:  0,
					exp:  []byte("test"),
					expN: 4,
				},
			},
		},
		{
			name: "new, set, open then get",
			ops: []op{
				blkNewOp{
					blk: &blk,

					size: 1 << 10,
				},
				blkWriteOp{
					blk: &blk,

					data: []byte("test"),
					off:  0,

					expN: 4,
				},
				blkOpenOp{
					blk: &blk2,
				},
				blkReadOp{
					blk: &blk,

					off:  0,
					exp:  []byte("test"),
					expN: 4,
				},
				blkReadOp{
					blk: &blk2,

					off:  0,
					exp:  []byte("test"),
					expN: 4,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, mktest(tc))
	}
}
