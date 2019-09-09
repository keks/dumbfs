package rwablk

import (
	"io"

	"github.com/keks/dumbfs"
)

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
