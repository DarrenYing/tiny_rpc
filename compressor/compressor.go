package compressor

// CompressType type of compressions supported by rpc
type CompressType uint16

// Compressor is interface, each compressor has Zip and Unzip functions
type Compressor interface {
	Zip([]byte) ([]byte, error)
	Unzip([]byte) ([]byte, error)
}

const (
	Raw CompressType = iota
	Gzip
	Snappy
	Zlib
)

var Compressors = map[CompressType]Compressor{
	Raw:    RawCompressor{},
	Gzip:   GzipCompressor{},
	Snappy: SnappyCompressor{},
	Zlib:   ZlibCompressor{},
}
