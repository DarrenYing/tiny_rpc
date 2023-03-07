package codec

import (
	"encoding/binary"
	"io"
	"net"
)

// sendFrame 向IO中写入uvarint类型的 size ，表示要发送数据的长度，随后将该字节slice类型的数据 data 写入IO流中
func sendFrame(w io.Writer, data []byte) (err error) {
	var size [binary.MaxVarintLen64]byte

	// 写入数据长度为0
	if len(data) == 0 {
		n := binary.PutUvarint(size[:], uint64(0))
		if err = write(w, size[:n]); err != nil {
			return
		}
		return
	}

	// 先向IO流写入数据长度，再写入数据内容
	n := binary.PutUvarint(size[:], uint64(len(data)))
	if err = write(w, size[:n]); err != nil {
		return
	}
	if err = write(w, data); err != nil {
		return
	}
	return
}

// recvFrame 从IO中读取uvarint类型的 size ，表示要接收数据的长度，随后将该从IO流中读取该 size 长度字节串
func recvFrame(r io.Reader) (data []byte, err error) {
	size, err := binary.ReadUvarint(r.(io.ByteReader))
	if err != nil {
		return nil, err
	}
	if size != 0 {
		data = make([]byte, size)
		if err = read(r, data); err != nil {
			return nil, err
		}
	}

	return data, err
}

// write the data into IO stream
func write(w io.Writer, data []byte) error {
	for index := 0; index < len(data); {
		n, err := w.Write(data[index:])
		if _, ok := err.(net.Error); !ok {
			return err
		}
		index += n
	}
	return nil
}

// read from IO stream into byte array
func read(r io.Reader, data []byte) error {
	for index := 0; index < len(data); {
		n, err := r.Read(data[index:])
		if err != nil {
			if _, ok := err.(net.Error); !ok {
				return err
			}
		}
		index += n
	}
	return nil
}
