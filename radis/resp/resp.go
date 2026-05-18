package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RESPType int

const (
	SimpleString RESPType = iota
	Error
	Integer
	BulkString
	Array
)

type RESPValue struct {
	Type   RESPType
	Str    string
	Num    int64
	Array  []RESPValue
	IsNull bool
}

func CreateArray(values ...string) RESPValue {
	array := make([]RESPValue, len(values))
	for i, value := range values {
		array[i] = CreateBulkString(value)
	}
	return RESPValue{Type: Array, Array: array}
}

func CreateBulkString(value string) RESPValue {
	return RESPValue{Type: BulkString, Str: value}
}

func CreateInteger(value int64) RESPValue {
	return RESPValue{Type: Integer, Num: value}
}

func CreateSimpleString(value string) RESPValue {
	return RESPValue{Type: SimpleString, Str: value}
}

func CreateErrorMessage(value string) RESPValue {
	return RESPValue{Type: Error, Str: value}
}

func CreateNull() RESPValue {
	return RESPValue{Type: BulkString, IsNull: true}
}

func (v RESPValue) Serialize() []byte {
	switch v.Type {
	case SimpleString:
		return fmt.Appendf(nil, "+%s\r\n", v.Str)
	case Error:
		return fmt.Appendf(nil, "-%s\r\n", v.Str)
	case Integer:
		return fmt.Appendf(nil, ":%d\r\n", v.Num)
	case BulkString:
		{
			if v.IsNull {
				return []byte("$-1\r\n")
			}
			return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		}
	case Array:
		{
			if v.IsNull {
				return []byte("*-1\r\n")
			}
			s := fmt.Sprintf("*%d\r\n", len(v.Array))
			for _, val := range v.Array {
				s += string(val.Serialize())
			}
			return []byte(s)
		}
	}
	return nil
}

func readLineAndTrim(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func ParseRESP(r *bufio.Reader) (RESPValue, error) {

	prefix, err := r.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	switch prefix {
	case '+':
		return parseSimpleString(r)
	case '-':
		return parseError(r)
	case ':':
		return parseInteger(r)
	case '$':
		return parseBulkString(r)
	case '*':
		return parseArray(r)
	}

	return RESPValue{}, fmt.Errorf("invalid prefix: %c", prefix)
}

func parseSimpleString(r *bufio.Reader) (RESPValue, error) {
	line, err := readLineAndTrim(r)
	if err != nil {
		return RESPValue{}, err
	}

	return RESPValue{Type: SimpleString, Str: line}, nil
}

func parseError(r *bufio.Reader) (RESPValue, error) {
	line, err := readLineAndTrim(r)
	if err != nil {
		return RESPValue{}, err
	}

	return RESPValue{Type: Error, Str: line}, nil
}

func parseInteger(r *bufio.Reader) (RESPValue, error) {
	line, err := readLineAndTrim(r)
	if err != nil {
		return RESPValue{}, err
	}

	num, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return RESPValue{}, err
	}

	return RESPValue{Type: Integer, Num: num}, nil
}

func parseBulkString(r *bufio.Reader) (RESPValue, error) {
	line, err := readLineAndTrim(r)
	if err != nil {
		return RESPValue{}, err
	}

	length, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return RESPValue{}, err
	}

	if length == -1 {
		return RESPValue{Type: BulkString, IsNull: true}, nil
	}

	if length < 0 {
		return RESPValue{}, fmt.Errorf("invalid bulk string length: %d", length)
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return RESPValue{}, err
	}

	if int64(n) != length {
		return RESPValue{}, fmt.Errorf("expected %d bytes, got %d", length, int64(n))
	}

	r.Discard(2)

	return RESPValue{Type: BulkString, Str: string(buf)}, nil
}

func parseArray(r *bufio.Reader) (RESPValue, error) {
	line, err := readLineAndTrim(r)
	if err != nil {
		return RESPValue{}, err
	}

	length, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return RESPValue{}, err
	}

	if length == -1 {
		return RESPValue{Type: Array, IsNull: true}, nil
	}

	if length < 0 {
		return RESPValue{}, fmt.Errorf("invalid array length: %d", length)
	}

	array := make([]RESPValue, length)
	for i := range array {
		val, err := ParseRESP(r)
		if err != nil {
			return RESPValue{}, err
		}
		array[i] = val
	}

	return RESPValue{Type: Array, Array: array}, nil
}
