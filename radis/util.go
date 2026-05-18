package radis

import (
	"bufio"
	"net"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

func readMessage(conn net.Conn) (resp.RESPValue, error) {
	reader := bufio.NewReader(conn)
	val, err := resp.ParseRESP(reader)
	if err != nil {
		return resp.RESPValue{}, err
	}
	return val, nil
}

func writeMessage(conn net.Conn, message resp.RESPValue) error {
	_, err := conn.Write(message.Serialize())
	if err != nil {
		return err
	}
	return nil
}
