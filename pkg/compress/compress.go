package compress

import (
	"github.com/pierrec/lz4"
)

func Compress(src, dst []byte, typ int) ([]byte, error) {
	switch typ {
	case Lz4:
		n, err := lz4.CompressBlock(src, dst, nil)
		if err != nil {
			return nil, err
		}
		return dst[:n], nil
	}
	return nil, nil
}

func Decompress(src, dst []byte, typ int) ([]byte, error) {
	switch typ {
	case Lz4:
		n, err := lz4.UncompressBlock(src, dst)
		if err != nil {
			return nil, err
		}
		return dst[:n], nil
	}
	return nil, nil
}
