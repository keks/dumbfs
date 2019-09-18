package blkfile

import (
	"bytes"
	"testing"

	"github.com/keks/dumbfs"
	"github.com/stretchr/testify/require"
)

// op is an operation performed during the test.
//
type op interface {
	Do(*testing.T, interface{})
}

func getRWA(t *testing.T, v interface{}) dumbfs.ReadWriterAt {
	rwa, ok := v.(dumbfs.ReadWriterAt)
	require.Truef(t, ok, "expected to be of type %T but got %T", rwa, v)

	return rwa
}

type blksNewOp struct {
	blks *Blocks

	expErr string
}

func (op blksNewOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)
	blks, err := New(rwa)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	// this copies a mutex, but it hasn't been used yet so it's okay
	*op.blks = *blks
}

type blksOpenOp struct {
	blks *Blocks

	expErr string
}

func (op blksOpenOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)
	blks, err := Open(rwa)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	// this copies a mutex, but it hasn't been used yet so it's okay
	*op.blks = *blks
}

type blksAllocateOp struct {
	blks *Blocks
	size int

	blk   *block
	blkid *dumbfs.BlockID

	expBid dumbfs.BlockID
	expErr string
}

func (op blksAllocateOp) Do(t *testing.T, v interface{}) {
	blkid, blk, err := op.blks.Allocate(op.size)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	require.Equal(t, op.expBid, blkid, "block id returned by allocate")

	*op.blk = *(blk.(*block))
	*op.blkid = blkid
}

type blksGetOp struct {
	blks  *Blocks
	blk   *block
	blkid *dumbfs.BlockID

	expErr string
}

func (op blksGetOp) Do(t *testing.T, v interface{}) {
	t.Logf("blocks get bid=%d", *op.blkid)
	blk, err := op.blks.Get(*op.blkid)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	*op.blk = *(blk.(*block))
}

type blkOpenOp struct {
	blk *block
	id  dumbfs.BlockID

	expErr string
}

func (op blkOpenOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)
	blk, err := openBlock(rwa, int64(op.id))
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	*op.blk = *blk
}

type blkNewOp struct {
	blk  *block
	size int
	id   dumbfs.BlockID

	expErr string
}

func (op blkNewOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)
	blk, err := newBlock(rwa, int64(op.id), op.size)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}

	*op.blk = *blk
}

type blkWriteOp struct {
	blk  *block
	data []byte
	off  int64

	// set these if blk == nil
	blkOff  int64
	blkSize int

	expN   int
	expErr string
}

func (op blkWriteOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)

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

	require.Equal(t, op.expN, n)
	if op.expErr == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, op.expErr)
	}
}

type blkReadOp struct {
	blk     *block
	off     int64
	readlen int

	// set these if blk == nil
	blkOff  int64
	blkSize int

	exp    []byte
	expN   int
	expErr string
}

func (op blkReadOp) Do(t *testing.T, v interface{}) {
	rwa := getRWA(t, v)
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

	buf := make([]byte, op.readlen)
	n, err := op.blk.ReadAt(buf, op.off)

	t.Logf("readOp, n: %d, err: %v", n, err)

	if op.expErr == "" {
		r.NoError(err)
	} else {
		r.EqualError(err, op.expErr)
	}
	r.Equal(op.expN, n)
	t.Logf("buffer contents %q | 0x%x", buf[:op.expN], buf[:op.expN])
	r.True(bytes.Equal(buf[:op.expN], op.exp))
}
