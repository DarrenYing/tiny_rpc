package codec

import (
	"bufio"
	"hash/crc32"
	"io"
	"net/rpc"
	"sync"
	"tiny_rpc/compressor"
	"tiny_rpc/header"
	"tiny_rpc/serializer"
)

type clientCodec struct {
	reader io.Reader
	writer io.Writer
	closer io.Closer

	compressor compressor.CompressType // rpc compress type
	serializer serializer.Serializer
	response   header.ResponseHeader // response header
	mutex      sync.Mutex            // protect pending map
	pending    map[uint64]string
}

// NewClientCodec Create a new client codec
func NewClientCodec(conn io.ReadWriteCloser, compressType compressor.CompressType, serializer serializer.Serializer) rpc.ClientCodec {
	return &clientCodec{
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		closer:     conn,
		compressor: compressType,
		serializer: serializer,
		pending:    make(map[uint64]string),
	}
}

// WriteRequest Write the rpc request header and body to the io stream
func (c *clientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
	// map 不是并发安全的
	c.mutex.Lock()
	c.pending[r.Seq] = r.ServiceMethod
	c.mutex.Unlock()

	if _, ok := compressor.Compressors[c.compressor]; !ok {
		return NotFoundCompressorError
	}

	// 将参数编码为请求体
	reqBody, err := c.serializer.Marshal(param)
	if err != nil {
		return err
	}
	// 压缩请求体
	compressedReqBody, err := compressor.Compressors[c.compressor].Zip(reqBody)
	if err != nil {
		return err
	}
	// 从请求头部对象池取出请求头
	h := header.RequestPool.Get().(*header.RequestHeader)
	// 循环利用请求头
	defer func() {
		h.ResetHeader()
		header.RequestPool.Put(h)
	}()

	h.ID = r.Seq
	h.Method = r.ServiceMethod
	h.RequestLen = uint32(len(compressedReqBody))
	h.CompressType = c.compressor
	h.Checksum = crc32.ChecksumIEEE(compressedReqBody)

	// 发送请求头
	if err := sendFrame(c.writer, h.Marshal()); err != nil {
		return err
	}
	// 发送请求体
	if err := write(c.writer, compressedReqBody); err != nil {
		return err
	}

	c.writer.(*bufio.Writer).Flush()
	return nil
}

// ReadResponseHeader read the rpc response header from the io stream
func (c *clientCodec) ReadResponseHeader(response *rpc.Response) error {
	c.response.ResetHeader()
	// 读取响应头
	data, err := recvFrame(c.reader)
	if err != nil {
		return err
	}
	// 解码响应头
	err = c.response.Unmarshal(data)
	if err != nil {
		return err
	}
	c.mutex.Lock()
	response.Seq = c.response.ID // 取出序列号
	response.Error = c.response.Error
	response.ServiceMethod = c.pending[response.Seq] // 取出响应方法
	delete(c.pending, response.Seq)                  // 删除pending中的序号
	c.mutex.Unlock()
	return nil
}

// ReadResponseBody read the rpc response body from the io stream
func (c *clientCodec) ReadResponseBody(param any) error {
	if param == nil {
		if c.response.ResponseLen != 0 { // 废弃多余部分
			if err := read(c.reader, make([]byte, c.response.ResponseLen)); err != nil {
				return err
			}
		}
		return nil
	}

	// 根据响应体长度，读取该长度的字节串
	respBody := make([]byte, c.response.ResponseLen)
	err := read(c.reader, respBody)
	if err != nil {
		return err
	}

	// 检查校验和
	if c.response.Checksum != 0 {
		if crc32.ChecksumIEEE(respBody) != c.response.Checksum {
			return UnexpectedChecksumError
		}
	}
	// 检查Compressor
	if c.response.GetCompressType() != c.compressor {
		return CompressorTypeMismatchError
	}
	// 解压响应体
	resp, err := compressor.Compressors[c.response.GetCompressType()].Unzip(respBody)
	if err != nil {
		return err
	}
	// 反序列化
	return c.serializer.Unmarshal(resp, param)
}

func (c *clientCodec) Close() error {
	return c.closer.Close()
}
