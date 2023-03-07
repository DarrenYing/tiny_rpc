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

type reqCtx struct {
	requestId    uint64
	compressType compressor.CompressType
}

type serverCodec struct {
	reader io.Reader
	writer io.Writer
	closer io.Closer

	request    header.RequestHeader
	serializer serializer.Serializer
	mutex      sync.Mutex // protects seq, pending
	seq        uint64
	pending    map[uint64]*reqCtx
}

// NewServerCodec Create a new server codec
func NewServerCodec(conn io.ReadWriteCloser, serializer serializer.Serializer) rpc.ServerCodec {
	return &serverCodec{
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		closer:     conn,
		serializer: serializer,
		pending:    make(map[uint64]*reqCtx),
	}
}

// ReadRequestHeader read the rpc request header from the io stream
func (s *serverCodec) ReadRequestHeader(request *rpc.Request) error {
	s.request.ResetHeader()
	// 读取请求头
	data, err := recvFrame(s.reader)
	if err != nil {
		return err
	}
	// 解码请求头
	err = s.request.Unmarshal(data)
	if err != nil {
		return err
	}

	s.mutex.Lock()
	s.seq++                     // 序号自增
	s.pending[s.seq] = &reqCtx{ // 自增序号和请求的上下文绑定
		requestId:    s.request.ID,
		compressType: s.request.GetCompressType(),
	}
	request.ServiceMethod = s.request.Method
	request.Seq = s.seq
	s.mutex.Unlock()
	return nil
}

// ReadRequestBody read the rpc request body from the io stream
func (s *serverCodec) ReadRequestBody(param any) error {
	if param == nil {
		if s.request.RequestLen != 0 {
			if err := read(s.reader, make([]byte, s.request.RequestLen)); err != nil {
				return err
			}
		}
		return nil
	}

	// 根据请求体长度，读取该长度的字节串
	reqBody := make([]byte, s.request.RequestLen)
	err := read(s.reader, reqBody)
	if err != nil {
		return err
	}

	// 检查校验和
	if s.request.Checksum != 0 {
		if crc32.ChecksumIEEE(reqBody) != s.request.Checksum {
			return UnexpectedChecksumError
		}
	}
	// 查看请求的压缩器是否已实现
	if _, ok := compressor.Compressors[s.request.GetCompressType()]; !ok {
		return NotFoundCompressorError
	}
	// 解压请求体
	req, err := compressor.Compressors[s.request.GetCompressType()].Unzip(reqBody)
	if err != nil {
		return err
	}
	// 反序列化
	return s.serializer.Unmarshal(req, param)

}

// WriteResponse Write the rpc response header and body to the io stream
func (s *serverCodec) WriteResponse(response *rpc.Response, param any) error {
	s.mutex.Lock()
	reqCtx, ok := s.pending[response.Seq]
	if !ok {
		s.mutex.Unlock()
		return InvalidSequenceError
	}
	delete(s.pending, response.Seq)
	s.mutex.Unlock()

	if response.Error != "" {
		param = nil
	}
	// 检查压缩器
	if _, ok := compressor.Compressors[reqCtx.compressType]; !ok {
		return NotFoundCompressorError
	}

	var respBody []byte
	var err error
	// 将参数编码为响应体
	if param != nil {
		respBody, err = s.serializer.Marshal(param)
		if err != nil {
			return err
		}
	}
	// 压缩响应体
	compressedRespBody, err := compressor.Compressors[reqCtx.compressType].Zip(respBody)
	if err != nil {
		return err
	}
	// 从响应头部对象池取出响应头
	h := header.ResponsePool.Get().(*header.ResponseHeader)
	defer func() {
		h.ResetHeader()
		header.ResponsePool.Put(h)
	}()

	h.ID = reqCtx.requestId
	h.Error = response.Error
	h.ResponseLen = uint32(len(compressedRespBody))
	h.Checksum = crc32.ChecksumIEEE(compressedRespBody)
	h.CompressType = reqCtx.compressType

	// 发送响应头
	if err = sendFrame(s.writer, h.Marshal()); err != nil {
		return err
	}
	// 发送响应体
	if err = write(s.writer, compressedRespBody); err != nil {
		return err
	}

	s.writer.(*bufio.Writer).Flush()
	return nil

}

func (s *serverCodec) Close() error {
	return s.closer.Close()
}
