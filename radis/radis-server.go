package radis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

type RadisServer struct {
	address  string
	listener net.Listener
	data     map[string]string
	mu       sync.RWMutex
}

func NewRadisServer(address string) *RadisServer {
	return &RadisServer{
		address: address,
		data:    make(map[string]string),
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

	for {
		val, err := resp.ParseRESP(bufio.NewReader(conn))
		if err != nil {
			break
		}

		switch val.Type {
		case resp.Array:
			if len(val.Array) == 0 {
				response := resp.RESPValue{Type: resp.Array, IsNull: true}
				conn.Write(response.Serialize())
			} else {
				command := val.Array[0].Str
				args := val.Array[1:]
				fmt.Println("Received command:", command)
				s.handleCommand(conn, command, args)
			}
		}
	}
}

func (s *RadisServer) Set(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 2 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].Str
	value := args[1].Str
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return resp.RESPValue{Type: resp.SimpleString, Str: "OK"}
}

func (s *RadisServer) Get(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 1 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Str
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]

	if !ok {
		return resp.RESPValue{Type: resp.BulkString, IsNull: true}
	}
	return resp.RESPValue{Type: resp.BulkString, Str: value}
}

func (s *RadisServer) Ping(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 0 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'ping' command"}
	}
	return resp.RESPValue{Type: resp.SimpleString, Str: "PONG"}
}

func (s *RadisServer) Echo(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 1 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'echo' command"}
	}
	return resp.RESPValue{Type: resp.BulkString, Str: args[0].Str}
}

func (s *RadisServer) handleCommand(conn net.Conn, command string, args []resp.RESPValue) {
	switch strings.ToUpper(command) {
	case "PING":
		response := s.Ping(args)
		conn.Write(response.Serialize())
	case "ECHO":
		response := s.Echo(args)
		conn.Write(response.Serialize())
	case "SET":
		response := s.Set(args)
		conn.Write(response.Serialize())
	case "GET":
		response := s.Get(args)
		conn.Write(response.Serialize())
	default:
		response := resp.RESPValue{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", command)}
		conn.Write(response.Serialize())
	}

}
