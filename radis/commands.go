package radis

import (
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

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
