package rwablk

import (
	"encoding/binary"
	"sync"

	"github.com/keks/dumbfs"
)

// TODO: keep track of opened blocks
// TODO: variable block sizes

type Blocks struct {
	l sync.Mutex

	lower dumbfs.ReadWriterAt

	next    int64
	blksize int
}

func (blks *Blocks) writeMeta() error {
	meta := make([]byte, 8+4)

	binary.LittleEndian.PutUint64(meta, uint64(blks.next))
	binary.LittleEndian.PutUint32(meta[8:], uint32(blks.blksize))

	_, err := blks.lower.WriteAt(meta, 0)
	return err
}

func (blks *Blocks) Get(bid dumbfs.BlockID) (dumbfs.ReadWriterAt, error) {
	return &block{
		off:   int64(bid),
		size:  blks.blksize,
		lower: blks.lower,
	}, nil
}

func (blks *Blocks) New() (dumbfs.BlockID, dumbfs.ReadWriterAt, error) {
	blks.l.Lock()
	defer blks.l.Unlock()

	bid := dumbfs.BlockID(blks.next)
	blks.next += int64(blks.blksize)

	err := blks.writeMeta()
	if err != nil {
		return 0, nil, err
	}

	blk, err := blks.Get(bid)
	return bid, blk, err
}


