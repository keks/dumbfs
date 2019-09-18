package blkfile

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/keks/dumbfs"
	"github.com/keks/testops"
)

func TestBlocks(t *testing.T) {
	var (
		envs = []testops.Env{
			testops.Env{
				Name: "mem",
				Func: func(tc testops.TestCase) (func(*testing.T), error) {
					rwa := &testReadWriterAt{[]byte{}}
					return tc.Runner(rwa), nil
				},
			},
			testops.Env{
				Name: "file",
				Func: func(tc testops.TestCase) (func(*testing.T), error) {
					f, err := ioutil.TempFile("", "TestBlock-*")

					return func(t *testing.T) {
						defer os.Remove(f.Name())
						tc.Runner(f)(t)
					}, err
				},
			},
		}

		blk, blk2, blk3 block
		blks, blks2     Blocks
		blkid           dumbfs.BlockID

		tcs = []testops.TestCase{
			{
				Name: "set then get",
				Ops: []testops.Op{
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
				Name: "set long then get short",
				Ops: []testops.Op{
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
				Name: "write over block end",
				Ops: []testops.Op{
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
				Name: "write after block end",
				Ops: []testops.Op{
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
				Name: "read over inner end",
				Ops: []testops.Op{
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
				Name: "read over block end",
				Ops: []testops.Op{
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
				Name: "read after block end",
				Ops: []testops.Op{
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
				Name: "new, set then get",
				Ops: []testops.Op{
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
				Name: "new, set, open then get",
				Ops: []testops.Op{
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
				Name: "new blocks, get block, write, read, reopen, read",
				Ops: []testops.Op{
					blksNewOp{
						blks: &blks,
					},
					testops.DumpOp{Name: "blks", V: &blks},
					blksAllocateOp{
						blks: &blks,
						size: 1 << 10,

						blk:   &blk,
						blkid: &blkid,

						expBid: dumbfs.BlockID(BlocksMetaSize + BlockHeaderSize),
					},
					testops.DumpOp{Name: "blkid", V: &blkid},
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
					testops.DumpOp{Name: "blk", V: &blk},
					testops.DumpOp{Name: "blk2", V: &blk2},
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
			{ // TODO improve errors
				Name: "open empty blocks fails",
				Ops: []testops.Op{
					blksOpenOp{
						blks:   &blks,
						expErr: "EOF",
					},
				},
			},
		}
	)

	testops.Run(t, envs, tcs)
}
