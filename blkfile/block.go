package blkfile

import (
	"encoding/binary"
	"io"

	"github.com/keks/dumbfs"
)

// BlockHeaderSize is the size of the header of each block in bytes.
const BlockHeaderSize = 4

// TODO: maybe export block?

func openBlock(rwa dumbfs.ReadWriterAt, off int64) (*block, error) {
	blk := &block{
		lower: rwa,
		off:   off,
	}

	return blk, blk.parse()
}
func newBlock(rwa dumbfs.ReadWriterAt, off int64, size int) (*block, error) {
	blk := &block{
		lower: rwa,
		off:   off,
		size:  size,
	}

	return blk, blk.format()
}

type block struct {
	off  int64
	size int

	lower dumbfs.ReadWriterAt
}

func (blk *block) ReadAt(dst []byte, off int64) (int, error) {
	if off >= int64(blk.size) {
		return 0, io.EOF
	}

	max := blk.size - int(off)
	var retEOF bool
	if max < len(dst) {
		dst = dst[:max]
		retEOF = true
	}

	n, err := blk.lower.ReadAt(dst, off+blk.off)
	if err != nil {
		return n, err
	}

	// return EOF if the caller wanted to read beyond the end of the block
	if retEOF {
		return n, io.EOF
	}

	return n, nil
}

func (blk *block) WriteAt(data []byte, off int64) (int, error) {
	if off >= int64(blk.size) {
		// TODO: is this the correct error?
		return 0, io.EOF
	}

	max := blk.size - int(off)
	var retErr bool
	if max < len(data) {
		data = data[:max]
		retErr = true
	}

	n, err := blk.lower.WriteAt(data, off+blk.off)
	if err != nil {
		// NOTE: this is only expected if the lower layer has failures,
		//       like e.g. running out of disk space.
		return n, err
	}

	// return EOF if the caller wanted to read beyond the end of the block
	if retErr {
		// TODO: is this the correct error?
		return n, io.EOF
	}

	return n, nil
}

func (blk *block) parse() error {
	var size int32
	err := binary.Read(readerFromReaderAt(blk.lower, blk.off), binary.LittleEndian, &size)
	if err != nil {
		return err
	}

	blk.size = int(size)

	blk.size -= BlockHeaderSize
	blk.off += BlockHeaderSize

	return nil
}

func (blk *block) format() error {
	size := make([]byte, BlockHeaderSize)
	binary.LittleEndian.PutUint32(size, uint32(blk.size))
	_, err := blk.lower.WriteAt(size, blk.off)
	if err != nil {
		return err
	}

	blk.size -= BlockHeaderSize
	blk.off += BlockHeaderSize

	return nil
}
