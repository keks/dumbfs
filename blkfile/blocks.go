package blkfile

import (
	"encoding/binary"
	"sync"

	"github.com/keks/dumbfs"
)

var (
	BlocksMetaSize int
)

func init() {
	BlocksMetaSize = binary.Size(int64(0))
}

func New(rwa dumbfs.ReadWriterAt) (*Blocks, error) {
	blks := &Blocks{
		lower: rwa,
	}

	return blks, blks.format()
}

func Open(rwa dumbfs.ReadWriterAt) (*Blocks, error) {
	blks := &Blocks{
		lower: rwa,
	}

	return blks, blks.parse()
}

type Blocks struct {
	l sync.Mutex

	lower dumbfs.ReadWriterAt

	next int64
}

// Allocate returns a new block that takes blksize bytes of space.
// Note that due to the four byte block header, the usable size will be lower.
func (blks *Blocks) Allocate(blksize int) (dumbfs.BlockID, dumbfs.ReadWriterAt, error) {
	blks.l.Lock()
	defer blks.l.Unlock()

	return blks.allocate(blksize)
}

func (blks *Blocks) allocate(blksize int) (dumbfs.BlockID, dumbfs.ReadWriterAt, error) {
	off := blks.next
	blks.next += int64(blksize)

	err := blks.writeMeta()
	if err != nil {
		return 0, nil, err
	}

	blk, err := newBlock(blks.lower, off, blksize)
	return dumbfs.BlockID(off), blk, err
}

func (blks *Blocks) Get(bid dumbfs.BlockID) (dumbfs.ReadWriterAt, error) {
	return openBlock(blks.lower, int64(bid))
}

func (blks *Blocks) writeMeta() error {
	metaBlk, err := blks.Get(0)
	if err != nil {
		return err
	}

	metaBlkW := funcWriter(func(data []byte) (int, error) {
		return metaBlk.WriteAt(data, 0)
	})

	return binary.Write(metaBlkW, binary.LittleEndian, blks.next)
}

func (blks *Blocks) format() error {
	cnt := BlocksMetaSize + BlockHeaderSize

	_, err := newBlock(blks.lower, 0, cnt)
	if err != nil {
		return err
	}

	_, _, err = blks.allocate(cnt)
	return err
}

func (blks *Blocks) parse() error {
	metaBlk, err := blks.Get(0)
	if err != nil {
		return err
	}

	metaBlkR := funcReader(func(buf []byte) (int, error) {
		return metaBlk.ReadAt(buf, 0)
	})

	return binary.Read(metaBlkR, binary.LittleEndian, &blks.next)
}
