package radis

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
	address    string
	listener   net.Listener
	data       map[string]StoreItem
	lists      map[string][]string
	mu         sync.RWMutex
	role       Role
	masterHost string
	masterPort string
	replId     string
	replOffset int64
}

type ServerConfig struct {
	Address   string
	ReplicaOf string
}

func NewRadisServer(config ServerConfig) *RadisServer {
	log.Println("Starting Radis server on", config.Address)
	role := Master

	if config.ReplicaOf != "" {
		role = Slave
		parts := strings.Split(config.ReplicaOf, ":")

		host := parts[0]
		port := parts[1]

		log.Println("Starting slave server with master at", host, port)

		return &RadisServer{
			address:    config.Address,
			data:       make(map[string]StoreItem),
			lists:      make(map[string][]string),
			role:       role,
			masterHost: host,
			masterPort: port,
		}
	}

	return &RadisServer{
		address:    config.Address,
		data:       make(map[string]StoreItem),
		lists:      make(map[string][]string),
		role:       role,
		replId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replOffset: 0,
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

	if s.role == Slave {
		if err := s.handshakeWithMaster(); err != nil {
			return err
		}
	}

	return s.Serve()
}

func (s *RadisServer) handshakeWithMaster() error {
	conn, err := net.Dial("tcp", net.JoinHostPort(s.masterHost, s.masterPort))
	if err != nil {
		return fmt.Errorf("failed to dial master: %v", err)
	}
	defer conn.Close()

	ping := resp.RESPValue{Type: resp.Array, Array: []resp.RESPValue{resp.RESPValue{Type: resp.SimpleString, Str: "PING"}}}
	conn.Write(ping.Serialize())

	reader := bufio.NewReader(conn)
	val, err := resp.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to parse response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "PONG" {
		return fmt.Errorf("master did not respond with PONG")
	}

	log.Println("Handshake with master successful")
	return nil
}

func (s *RadisServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		val, err := resp.ParseRESP(reader)
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

func (s *RadisServer) Set(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 2 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].Str
	value := args[1].Str
	expiry := time.Time{}

	if len(args) > 2 {
		expiryCommand := args[2].Str
		if len(args) < 4 {
			return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'set' command"}
		}

		switch strings.ToUpper(expiryCommand) {
		case "EX":
			seconds, err := strconv.ParseInt(args[3].Str, 10, 64)
			if err != nil {
				return resp.RESPValue{Type: resp.Error, Str: "ERR invalid expiry time"}
			}

			expiry = time.Now().Add(time.Duration(seconds) * time.Second)
		case "PX":
			milliseconds, err := strconv.ParseInt(args[3].Str, 10, 64)
			if err != nil {
				return resp.RESPValue{Type: resp.Error, Str: "ERR invalid expiry time"}
			}

			expiry = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
		default:
			return resp.RESPValue{Type: resp.Error, Str: "ERR invalid expiry command"}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = StoreItem{value: value, expiry: expiry}

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

	if !value.expiry.IsZero() && time.Now().After(value.expiry) {
		return resp.RESPValue{Type: resp.BulkString, IsNull: true}
	}

	return resp.RESPValue{Type: resp.BulkString, Str: value.value}
}

func (s *RadisServer) RPush(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 2 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].Str
	values := args[1:]

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lists[key]; !ok {
		s.lists[key] = []string{}
	}
	for _, v := range values {
		s.lists[key] = append(s.lists[key], v.Str)
	}

	return resp.RESPValue{Type: resp.Integer, Num: int64(len(s.lists[key]))}
}

func (s *RadisServer) LRange(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 3 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'lrange' command"}
	}

	key := args[0].Str

	start, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.RESPValue{Type: resp.Error, Str: "ERR invalid start index"}
	}

	end, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return resp.RESPValue{Type: resp.Error, Str: "ERR invalid end index"}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.lists[key]
	if !ok {
		return resp.RESPValue{Type: resp.Array, IsNull: true}
	}

	if start < 0 {
		start = max(0, int64(len(list))+start)
	}

	if end < 0 {
		end = max(0, int64(len(list))+end)
	}

	if start > int64(len(list)) {
		return resp.RESPValue{Type: resp.Array, IsNull: true}
	}

	if end > int64(len(list)) {
		end = int64(len(list)) - 1
	}

	response := resp.RESPValue{Type: resp.Array, Array: []resp.RESPValue{}}
	for i := start; i <= end; i++ {
		response.Array = append(response.Array, resp.RESPValue{Type: resp.BulkString, Str: list[i]})
	}

	return response
}

func (s *RadisServer) LPush(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 2 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].Str
	values := args[1:]

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lists[key]; !ok {
		s.lists[key] = []string{}
	}
	for _, v := range values {
		s.lists[key] = append([]string{v.Str}, s.lists[key]...)
	}

	return resp.RESPValue{Type: resp.Integer, Num: int64(len(s.lists[key]))}
}

func (s *RadisServer) LLen(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 1 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].Str
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.lists[key]
	if !ok {
		return resp.RESPValue{Type: resp.Integer, Num: 0}
	}

	return resp.RESPValue{Type: resp.Integer, Num: int64(len(list))}
}

func (s *RadisServer) LPop(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 1 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'lpop' command"}
	}

	key := args[0].Str
	elementsToPop := 1

	if len(args) > 1 {
		elementsToPopInt, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return resp.RESPValue{Type: resp.Error, Str: "ERR invalid number of elements to pop"}
		}
		if elementsToPopInt < 1 {
			return resp.RESPValue{Type: resp.Error, Str: "ERR number of elements to pop must be greater than 0"}
		}

		elementsToPop = int(elementsToPopInt)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.lists[key]
	if !ok {
		return resp.RESPValue{Type: resp.BulkString, IsNull: true}
	}

	response := resp.RESPValue{Type: resp.Array, Array: []resp.RESPValue{}}

	if elementsToPop == 1 {
		firstElement := list[0]
		s.lists[key] = list[1:]
		return resp.RESPValue{Type: resp.BulkString, Str: firstElement}
	} else {
		for i := 0; i < elementsToPop; i++ {
			elementToRemove := list[i]
			response.Array = append(response.Array, resp.RESPValue{Type: resp.BulkString, Str: elementToRemove})
			s.lists[key] = list[i+1:]
		}
	}
	return response
}

func (s *RadisServer) Info(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 1 {
		return resp.RESPValue{Type: resp.Error, Str: "ERR wrong number of arguments for 'info' command"}
	}
	optionalArgument := args[0].Str
	switch strings.ToUpper(optionalArgument) {
	case "REPLICATION":
		if s.role == Master {
			return resp.RESPValue{Type: resp.BulkString, Str: fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", s.role, s.replId, s.replOffset)}
		} else {
			return resp.RESPValue{Type: resp.BulkString, Str: fmt.Sprintf("role:%s", s.role)}
		}
	default:
		return resp.RESPValue{Type: resp.Error, Str: "ERR unknown command"}
	}
}

func (s *RadisServer) handleCommand(conn net.Conn, command string, args []resp.RESPValue) {
	switch strings.ToUpper(command) {
	case "INFO":
		response := s.Info(args)
		conn.Write(response.Serialize())
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
