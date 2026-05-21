package radis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
)

type MasterServer struct {
	*RadisServer
	replicas  []net.Conn
	replicaMu sync.Mutex
	replId    string
	//TODO: update this
	replOffset string
}

func (m *MasterServer) Serve() error {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			fmt.Fprintf(os.Stderr, "accept error: %v\n", err)
			return err
		}
		go m.handleConnection(conn)
	}
}

func (m *MasterServer) Start() error {
	if err := m.Listen(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.checkReplicaState(ctx)
	return m.Serve()
}

func (m *MasterServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		val, err := resp.ParseRESP(reader)
		if err != nil {
			break
		}
		if val.Type == resp.Array && len(val.Array) > 0 {
			fmt.Println("Received command to Master:", val.Array[0].Str)
			m.handleCommand(conn, val.Array[0].Str, val.Array[1:], reader)
		}
	}
}

func (m *MasterServer) propogateToReplicas(command string, args []resp.RESPValue) {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	argStrings := make([]string, len(args))
	for i, arg := range args {
		argStrings[i] = arg.Str
	}
	allArgs := append([]string{command}, argStrings...)

	for _, replica := range m.replicas {
		replica.Write(resp.CreateArray(allArgs...).Serialize())
	}
}

func (m *MasterServer) Info(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 1 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'info' command")
	}
	optionalArgument := args[0].Str

	switch strings.ToUpper(optionalArgument) {
	case "REPLICATION":
		return resp.CreateBulkString(fmt.Sprintf("role:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%s", m.replId, m.replOffset))
	default:
		return resp.CreateErrorMessage("ERR unknown command")
	}
}

func (m *MasterServer) ReplConf(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 2 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'replconf' command")
	}

	command := args[0].Str

	switch strings.ToUpper(command) {
	case "LISTENING-PORT":
		return resp.CreateSimpleString("OK")
	case "CAPA":
		return resp.CreateSimpleString("OK")
	default:
		return resp.CreateErrorMessage(fmt.Sprintf("ERR unknown command '%s'", command))
	}
}

func (m *MasterServer) PSync(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 2 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'psync' command")
	}

	replId := args[0].Str
	replOffset := args[1].Str

	if replId == "?" {
		return resp.CreateSimpleString(fmt.Sprintf("FULLRESYNC %s %s", m.replId, m.replOffset))
	}

	replOffsetInt, err := strconv.Atoi(replOffset)
	if err != nil {
		return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid replication offset: %v", err))
	}
	sreplOffsetInt, err := strconv.Atoi(m.replOffset)
	if err != nil {
		return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid replication offset: %v", err))
	}
	if replOffsetInt > sreplOffsetInt {
		return resp.CreateSimpleString(fmt.Sprintf("CONTINUE %s", m.replOffset))
	}
	return resp.CreateSimpleString(fmt.Sprintf("FULLRESYNC %s %s", m.replId, m.replOffset))
}

func (m *MasterServer) FullSync(conn net.Conn) error {
	rdbContent, err := os.ReadFile("empty")
	if err != nil {
		rdbContent, err = os.ReadFile("../empty")
		if err != nil {
			return fmt.Errorf("failed to read empty RDB file: %v", err)
		}
	}

	header := fmt.Sprintf("$%d\r\n", len(rdbContent))
	conn.Write([]byte(header))
	conn.Write(rdbContent)
	return nil
}

func (m *MasterServer) handleCommand(conn net.Conn, command string, args []resp.RESPValue, reader *bufio.Reader) {
	switch strings.ToUpper(command) {
	case "SET":
		conn.Write(m.Set(args).Serialize())
		m.propogateToReplicas(command, args)
	case "INFO":
		conn.Write(m.Info(args).Serialize())
	case "REPLCONF":
		conn.Write(m.ReplConf(args).Serialize())
	case "PSYNC":
		conn.Write(m.PSync(args).Serialize())
		m.FullSync(conn)
		m.addReplica(conn)
		m.listenForReplicaAck(conn, reader)
		return
	default:
		m.RadisServer.handleCommand(conn, command, args)
	}
}

func (m *MasterServer) addReplica(conn net.Conn) {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	m.replicas = append(m.replicas, conn)
}

func (m *MasterServer) removeReplica(conn net.Conn) {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	for i, replica := range m.replicas {
		if replica == conn {
			m.replicas = append(m.replicas[:i], m.replicas[i+1:]...)
			break
		}
	}
	conn.Close()
}

func (m *MasterServer) listenForReplicaAck(conn net.Conn, reader *bufio.Reader) {
	for {
		val, err := resp.ParseRESP(reader)
		if err != nil {
			m.removeReplica(conn)
			return
		}
		if val.Type == resp.Array && len(val.Array) > 0 {
			command := val.Array[0].Str
			args := val.Array[1:]
			switch strings.ToUpper(command) {
			case "REPLCONF":
				if len(args) < 2 {
					fmt.Errorf("ERR wrong number of arguments for 'replconf' command")
				}

				switch strings.ToUpper(args[0].Str) {
				case "ACK":
					offset, err := strconv.ParseInt(args[1].Str, 10, 64)
					if err != nil {
						fmt.Errorf("ERR invalid replication offset: %v", err)
					}
					//TODO: have a better way to handle this
					m.replOffset = fmt.Sprintf("%d", offset)
				}

			}
		}
	}
}

func (m *MasterServer) checkReplicaState(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.replicaMu.Lock()
			cmd := resp.CreateArray("REPLCONF", "GETACK", "*")
			for _, replica := range m.replicas {
				replica.Write(cmd.Serialize())
			}
			m.replicaMu.Unlock()
		}

	}
}
