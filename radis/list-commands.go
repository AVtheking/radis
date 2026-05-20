package radis

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

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
	if len(list) == 0 {
		return response
	}

	if elementsToPop > len(list) {
		elementsToPop = len(list)
	}

	if elementsToPop == 1 {
		firstElement := list[0]
		s.lists[key] = list[1:]
		return resp.RESPValue{Type: resp.BulkString, Str: firstElement}
	} else {
		poppedElements := list[:elementsToPop]
		s.lists[key] = list[elementsToPop:]
		for _, element := range poppedElements {
			response.Array = append(response.Array, resp.RESPValue{Type: resp.BulkString, Str: element})
		}
	}
	return response
}
