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
	masterConn net.Conn
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

func (r *SlaveServer) ConnectToMaster() error {
	conn, err := r.handshakeWithMaster()
	if err != nil {
		return err
	}
	r.masterConn = conn
	go r.listenForMasterCommands(r.masterConn)
	return nil
}

func (r *SlaveServer) Start() error {
	if err := r.Listen(); err != nil {
		return err
	}
	if err := r.ConnectToMaster(); err != nil {
		return err
	}
	return r.Serve()
}

func (r *SlaveServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		val, err := resp.ParseRESP(reader)
		if err != nil {
			break
		}
		if val.Type == resp.Array && len(val.Array) > 0 {
			fmt.Println("Received command in Replica:", val.Array[0].Str)
			r.handleCommand(conn, val.Array[0].Str, val.Array[1:])
		}
	}
}

func (r *SlaveServer) currentReplicaOffset() int64 {
	offset, _ := strconv.ParseInt(r.replOffset, 10, 64)
	return offset
}

func (r *SlaveServer) incrementOffset(n int) {
	currentOffset := r.currentReplicaOffset()
	r.replOffset = strconv.FormatInt(currentOffset+int64(n), 10)
}

func (r *SlaveServer) listenForMasterCommands(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Listening for commands from master")
	reader := bufio.NewReader(conn)
	for {
		val, err := resp.ParseRESP(reader)
		if err != nil {
			break
		}
		if val.Type == resp.Array && len(val.Array) > 0 {
			fmt.Println("Received command from master:", val.Array[0].Str)
			commandBytes := len(val.Serialize())
			command := val.Array[0].Str
			args := val.Array[1:]
			switch strings.ToUpper(command) {
			case "PING":
				r.Ping(args)
				r.incrementOffset(commandBytes)
			case "SET":
				r.Set(args)
				r.incrementOffset(commandBytes)
			case "GET":
				r.Get(args)
				r.incrementOffset(commandBytes)
			case "RPUSH":
				r.RPush(args)
				r.incrementOffset(commandBytes)
			case "LRANGE":
				r.LRange(args)
				r.incrementOffset(commandBytes)
			case "LPUSH":
				r.LPush(args)
				r.incrementOffset(commandBytes)
			case "REPLCONF":
				r.ReplConf(args, conn)
				r.incrementOffset(commandBytes)
			default:
				fmt.Println("Unknown command from master:", command)
				return
			}
		}
	}
}

func (r *SlaveServer) ReplConf(args []resp.RESPValue, conn net.Conn) {
	if len(args) < 2 {
		fmt.Println("ERR wrong number of arguments for 'replconf' command")
		return
	}
	command := args[0].Str
	switch strings.ToUpper(command) {
	case "GETACK":
		conn.Write(resp.CreateArray("REPLCONF", "ACK", r.replOffset).Serialize())
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

func (r *SlaveServer) handshakeWithMaster() (net.Conn, error) {
	conn, err := net.Dial("tcp", net.JoinHostPort(r.masterHost, r.masterPort))
	if err != nil {
		return nil, fmt.Errorf("failed to dial master: %v", err)
	}

	reader := bufio.NewReader(conn)

	ping := resp.CreateArray("PING")
	writeMessage(conn, ping)

	val, err := resp.ParseRESP(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "PONG" {
		return nil, fmt.Errorf("master did not respond with PONG")
	}

	replConf := resp.CreateArray("replconf", "listening-port", r.address)
	writeMessage(conn, replConf)

	val, err = resp.ParseRESP(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "OK" {
		return nil, fmt.Errorf("master did not respond with OK")
	}
	log.Println("Replconf sent to master")

	replConf2 := resp.CreateArray("replconf", "capa", "psync2")
	writeMessage(conn, replConf2)

	val, err = resp.ParseRESP(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if val.Type != resp.SimpleString || val.Str != "OK" {
		return nil, fmt.Errorf("master did not respond with OK")
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
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	log.Println("PSync response:", val.Str)

	if val.Type != resp.SimpleString || !strings.HasPrefix(val.Str, "FULLRESYNC") {
		return nil, fmt.Errorf("master did not respond with FULLRESYNC")
	}

	b, err := reader.ReadByte()
	if err != nil || b != '$' {
		return nil, fmt.Errorf("expected $ in response, got %c", b)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %v", err)
	}

	length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse length: %v", err)
	}

	rdbContent := make([]byte, length)
	n, err := io.ReadFull(reader, rdbContent)
	if err != nil {
		return nil, fmt.Errorf("failed to read RDB content: %v", err)
	}

	if n != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, n)
	}

	log.Println("RDB content:", string(rdbContent))

	log.Println("Handshake with master successful")
	return conn, nil
}
