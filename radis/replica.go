package radis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

type SlaveServer struct {
	*RadisServer
	masterHost string
	masterPort string
	replId     string
	replOffset string
}

func (r *SlaveServer) Serve() error {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			return err
		}
		go r.handleConnection(conn)
	}
}

func (r *SlaveServer) Start() error {
	if err := r.Listen(); err != nil {
		return err
	}

	if err := r.handshakeWithMaster(); err != nil {
		return err
	}

	return r.Serve()
}

func (r *SlaveServer) handleConnection(conn net.Conn) {
    defer conn.Close()
    reader := bufio.NewReader(conn)
    for {
        val, err := resp.ParseRESP(reader)
        if err != nil { break }
        if val.Type == resp.Array && len(val.Array) > 0 {
			fmt.Println("Received command:", val.Array[0].Str)
            r.handleCommand(conn, val.Array[0].Str, val.Array[1:])
        }
    }
}

func (s *SlaveServer) handleCommand(conn net.Conn, command string, args []resp.RESPValue) {
	switch strings.ToUpper(command) {
	case "INFO":
		conn.Write(s.Info(args).Serialize())
	default:
		s.RadisServer.handleCommand(conn, command, args)
	}
}

func (s *SlaveServer) Info(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 1 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'info' command")
	}
	optionalArgument := args[0].Str

	switch strings.ToUpper(optionalArgument) {
	case "REPLICATION":
		return resp.CreateBulkString(fmt.Sprintf("role:slave"))
	default:
		return resp.CreateErrorMessage("ERR unknown command")
	}
}

func (r *SlaveServer) handshakeWithMaster() error {
	conn, err := net.Dial("tcp", net.JoinHostPort(r.masterHost, r.masterPort))
	if err != nil {
		return fmt.Errorf("failed to dial master: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	ping := resp.CreateArray("PING")
	writeMessage(conn, ping)

	val, err := resp.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to parse response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "PONG" {
		return fmt.Errorf("master did not respond with PONG")
	}

	replConf := resp.CreateArray("replconf", "listening-port", r.address)
	writeMessage(conn, replConf)

	val, err = resp.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "OK" {
		return fmt.Errorf("master did not respond with OK")
	}
	log.Println("Replconf sent to master")

	replConf2 := resp.CreateArray("replconf", "capa", "psync2")
	writeMessage(conn, replConf2)

	val, err = resp.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "OK" {
		return fmt.Errorf("master did not respond with OK")
	}
	log.Println("Replconf2 sent to master")

	replId := "?"
	if r.replId != "" {
		replId = r.replId
	}
	psyncCommand := resp.CreateArray("PSYNC", replId, r.replOffset)
	writeMessage(conn, psyncCommand)

	val, err = resp.ParseRESP(reader)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	log.Println("PSync response:", val.Str)

	if val.Type != resp.SimpleString || !strings.HasPrefix(val.Str, "FULLRESYNC") {
		return fmt.Errorf("master did not respond with FULLRESYNC")
	}

	b, err := reader.ReadByte()
	if err != nil || b != '$' {
		return fmt.Errorf("expected $ in response, got %c", b)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read length: %v", err)
	}

	length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return fmt.Errorf("failed to parse length: %v", err)
	}

	rdbContent := make([]byte, length)
	n, err := io.ReadFull(reader, rdbContent)
	if err != nil {
		return fmt.Errorf("failed to read RDB content: %v", err)
	}

	if n != length {
		return fmt.Errorf("expected %d bytes, got %d", length, n)
	}

	log.Println("RDB content:", string(rdbContent))

	log.Println("Handshake with master successful")
	return nil
}
