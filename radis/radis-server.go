package radis

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

type RadisServer struct {
	address  string
	listener net.Listener
}

func NewRadisServer(address string) *RadisServer {
	return &RadisServer{
		address: address,
	}
}

func (s *RadisServer) Listen() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

func (s *RadisServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *RadisServer) Close() error {
	return s.listener.Close()
}

func (s *RadisServer) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *RadisServer) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve()
}

func (s *RadisServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		if n == 0 {
			break
		}

		command := strings.TrimSpace(string(buf[:n]))
		parts := strings.Split(command, " ")
		
		switch parts[0] {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(parts) == 2 {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(parts[1]), parts[1])))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
			}
		}
	}
}
