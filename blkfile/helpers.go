package blkfile

import "io"

type funcWriter func([]byte) (int, error)

func (w funcWriter) Write(data []byte) (int, error) {
	return w(data)
}

func readerFromReaderAt(ra io.ReaderAt, off int64) io.Reader {
	return funcReader(func(data []byte) (int, error) {
		n, err := ra.ReadAt(data, off)
		off += int64(n)
		return n, err
	})
}

type funcReader func([]byte) (int, error)

func (r funcReader) Read(buf []byte) (int, error) {
	return r(buf)
}
