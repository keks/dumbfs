package blkfile

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/keks/dumbfs"
	"github.com/stretchr/testify/require"
)

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
				t.Logf("ok: %T", op)
			}

			// test with os.File as ReadWriterAt
			f, err := ioutil.TempFile("", "TestBlock-*")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			for _, op := range tc.ops {
				op.Do(t, f)
				t.Logf("ok: %T", op)
			}
		}
	}

	var (
		blk, blk2, blk3 block
		blks, blks2     Blocks
		blkid           dumbfs.BlockID
	)

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
					off:     (1 << 10) - 2,
					expN:    2,
					expErr:  "EOF",
					exp:     []byte("te"),
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
					data: []byte("test"),
					off:  0,
					expN: 4,

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:     2,
					expN:    2,
					expErr:  "EOF",
					exp:     []byte("st"),
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
					data: bytes.Repeat([]byte("test"), 1<<8),
					off:  0,
					expN: 1 << 10,

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:     (1 << 10) - 2,
					expN:    2,
					expErr:  "EOF",
					exp:     []byte("st"),
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
					data: bytes.Repeat([]byte("test"), 1<<8),
					off:  0,
					expN: 1 << 10,

					blkSize: 1 << 10,
				},
				blkReadOp{
					off:     (1 << 10) + 2,
					expN:    0,
					expErr:  "EOF",
					exp:     []byte(""),
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
		{
			name: "new blocks, get block, write, read, reopen, read",
			ops: []op{
				blksNewOp{
					blks: &blks,
				},
				dumpOp{"blks", &blks},
				blksAllocateOp{
					blks: &blks,
					size: 1 << 10,

					blk:   &blk,
					blkid: &blkid,

					expBid: dumbfs.BlockID(BlocksMetaSize + BlockHeaderSize),
				},
				dumpOp{"blkid", &blkid},
				blkWriteOp{
					blk:  &blk,
					data: []byte("test"),
					off:  0,

					expN: 4,
				},
				blksGetOp{
					blks:  &blks,
					blkid: &blkid,

					blk: &blk2,
				},
				dumpOp{"blk", &blk},
				dumpOp{"blk2", &blk2},
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
				blksOpenOp{
					blks: &blks2,
				},
				blksGetOp{
					blks:  &blks2,
					blk:   &blk3,
					blkid: &blkid,
				},
				blkReadOp{
					blk: &blk3,

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
