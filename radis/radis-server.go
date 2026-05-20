package radis

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

type Role string

const (
	Master Role = "master"
	Slave  Role = "slave"
)

type StoreItem struct {
	value  string
	expiry time.Time
}

type RadisServer struct {
	address  string
	listener net.Listener
	data     map[string]StoreItem
	lists    map[string][]string
	mu       sync.RWMutex
	role     Role
}

type ServerConfig struct {
	Address   string
	ReplicaOf string
}
type Server interface {
	Start() error
	Listen() error
	Serve() error
	Close() error
	Addr() string
}

func NewRadisServer(config ServerConfig) Server {
	base := &RadisServer{
		address: config.Address,
		data:    make(map[string]StoreItem),
		lists:   make(map[string][]string),
	}
	if config.ReplicaOf != "" {
		parts := strings.Split(config.ReplicaOf, " ")
		return &SlaveServer{
			RadisServer: base,
			masterHost:  parts[0],
			masterPort:  parts[1],
			replId:      "",
			replOffset:  "-1",
		}
	}
	return &MasterServer{
		RadisServer: base,
		replId:      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset:  "0",
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
	case "RPUSH":
		response := s.RPush(args)
		conn.Write(response.Serialize())
	case "LRANGE":
		response := s.LRange(args)
		fmt.Println("LRange response:", string(response.Serialize()))
		conn.Write(response.Serialize())
	case "LPUSH":
		response := s.LPush(args)
		conn.Write(response.Serialize())
	case "LLEN":
		response := s.LLen(args)
		conn.Write(response.Serialize())
	case "LPOP":
		response := s.LPop(args)
		conn.Write(response.Serialize())
	default:
		response := resp.RESPValue{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", command)}
		conn.Write(response.Serialize())
	}

}
