package blkfile

import (
	"io"
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
