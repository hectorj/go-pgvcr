package grecho

import (
	"bytes"
	"io"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

type bufferedTeeReader struct {
	buf    *bytes.Buffer
	writer io.Writer
	tee    io.Reader
}

func newBufferedTee(r io.Reader, w io.Writer) *bufferedTeeReader {
	buf := bytes.NewBuffer(make([]byte, 0, buffer.DefaultBufferSize))
	reader := io.TeeReader(r, buf)
	return &bufferedTeeReader{
		tee:    reader,
		buf:    buf,
		writer: w,
	}
}

func (b *bufferedTeeReader) Read(p []byte) (n int, err error) {
	return b.tee.Read(p)
}

func (b *bufferedTeeReader) Flush() (int, error) {
	bs := b.buf.Bytes()
	b.buf.Reset()
	return b.writer.Write(bs)
}
