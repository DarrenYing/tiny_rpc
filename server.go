package tiny_rpc

import (
	"log"
	"net"
	"net/rpc"
	"tiny_rpc/codec"
	"tiny_rpc/serializer"
)

// Server rpc server based on net/rpc implementation
type Server struct {
	*rpc.Server
	serializer.Serializer
}

// NewServer Create a new rpc server
func NewServer(opts ...Option) *Server {
	options := options{
		serializer: serializer.Proto,
	}

	for _, option := range opts {
		option(&options)
	}

	return &Server{
		Server:     &rpc.Server{},
		Serializer: options.serializer,
	}
}

// Register register rpc function
func (s *Server) Register(rcvr interface{}) error {
	return s.Server.Register(rcvr)
}

// RegisterName register the rpc function with the specified name
func (s *Server) RegisterName(name string, rcvr interface{}) error {
	return s.Server.RegisterName(name, rcvr)
}

func (s *Server) Serve(listener net.Listener) {
	log.Printf("tinyrpc started on: %s", listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go s.Server.ServeCodec(codec.NewServerCodec(conn, s.Serializer))
	}
}
