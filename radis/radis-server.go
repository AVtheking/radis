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

				switch strings.ToUpper(command) {
				case "PING":
					response := resp.RESPValue{Type: resp.SimpleString, Str: "PONG"}
					conn.Write(response.Serialize())
				case "ECHO":
					if len(args) != 1 {
						response := resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'echo' command"}
						conn.Write(response.Serialize())
					} else {
						response := resp.RESPValue{Type: resp.BulkString, Str: args[0].Str}
						conn.Write(response.Serialize())
					}
				case "SET":
					if len(args) < 2 {
						response := resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'set' command"}
						conn.Write(response.Serialize())
					} else {
						key := args[0].Str
						value := args[1].Str
						s.mu.Lock()
						s.data[key] = value
						s.mu.Unlock()
						response := resp.RESPValue{Type: resp.SimpleString, Str: "OK"}
						conn.Write(response.Serialize())
					}
				case "GET":
					if len(args) != 1 {
						response := resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'get' command"}
						conn.Write(response.Serialize())
					} else {
						key := args[0].Str
						s.mu.RLock()
						value, ok := s.data[key]
						s.mu.RUnlock()
						if !ok {
							response := resp.RESPValue{Type: resp.BulkString, IsNull: true}
							conn.Write(response.Serialize())
						} else {
							response := resp.RESPValue{Type: resp.BulkString, Str: value}
							conn.Write(response.Serialize())
						}
					}
				default:
					response := resp.RESPValue{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", command)}
					conn.Write(response.Serialize())
				}
			}
		}

	}
}
