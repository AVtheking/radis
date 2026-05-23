package radis

import (
	"bufio"
	"context"
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

type ReplicaState struct {
	conn   net.Conn
	offset int64
}
type MasterServer struct {
	*RadisServer
	replicaMu  sync.Mutex
	replId     string
	replicas   map[net.Conn]*ReplicaState
	replOffset int64
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
	data := resp.CreateArray(allArgs...).Serialize()
	for replicaConn, _ := range m.replicas {
		replicaConn.Write(data)
	}

	m.replOffset += int64(len(data))
}

func (m *MasterServer) Info(args []resp.RESPValue) resp.RESPValue {
	if len(args) < 1 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'info' command")
	}
	optionalArgument := args[0].Str

	switch strings.ToUpper(optionalArgument) {
	case "REPLICATION":
		return resp.CreateBulkString(fmt.Sprintf("role:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", m.replId, m.replOffset))
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
		if strings.Contains(args[1].Str, "psync2") {
			return resp.CreateSimpleString("OK")
		}
		return resp.CreateErrorMessage("ERR psync2 capability not found")
	default:
		return resp.CreateErrorMessage(fmt.Sprintf("ERR unknown command '%s'", command))
	}
}

func (m *MasterServer) PSync(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 2 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'psync' command")
	}

	replId := args[0].Str
	// replOffset := args[1].Str

	if replId == "?" {
		return resp.CreateSimpleString(fmt.Sprintf("FULLRESYNC %s %d", m.replId, m.replOffset))
	}

	// replOffsetInt, err := strconv.Atoi(replOffset)
	// if err != nil {
	// 	return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid replication offset: %v", err))
	// }
	// sreplOffsetInt, err := strconv.Atoi(m.replOffset)
	// if err != nil {
	// 	return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid replication offset: %v", err))
	// }
	// if replOffsetInt > sreplOffsetInt {
	// 	return resp.CreateSimpleString(fmt.Sprintf("CONTINUE %s", m.replOffset))
	// }
	return resp.CreateSimpleString(fmt.Sprintf("FULLRESYNC %s %d", m.replId, m.replOffset))
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

func (m *MasterServer) Wait(args []resp.RESPValue) resp.RESPValue {
	if len(args) != 2 {
		return resp.CreateErrorMessage("ERR wrong number of arguments for 'wait' command")
	}

	numReplica, err := strconv.Atoi(args[0].Str)
	if err != nil {
		return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid replica count: %v", err))
	}

	timeout, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.CreateErrorMessage(fmt.Sprintf("ERR invalid timeout: %v", err))
	}

	if numReplica == 0 {
		return resp.CreateInteger(0)
	}

	if timeout == 0 {
		return resp.CreateInteger(0)
	}

	targetOffset := m.currentMasterOffset()

	if targetOffset == 0 {
		return resp.CreateInteger(int64(m.connectednumReplica()))
	}

	m.requestReplicaAck()

	deadline := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	for time.Now().Before(deadline) {
		log.Println("\x1b[34m------------------Waiting for replicas to ACK within timeout--------------\x1b[0m")
		acked := m.countReplicasAtOffset(targetOffset)
		if acked >= numReplica {
			return resp.CreateInteger(int64(acked))
		}
		time.Sleep(10 * time.Millisecond)
	}

	return resp.CreateInteger(int64(m.connectednumReplica()))
}

func (m *MasterServer) addReplica(conn net.Conn) {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	m.replicas[conn] = &ReplicaState{conn: conn, offset: 0}
	log.Println("\x1b[33m------------------Added replica: ", conn.RemoteAddr().String(), "--------------\x1b[0m")
}

func (m *MasterServer) removeReplica(conn net.Conn) {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	delete(m.replicas, conn)
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

					log.Println("\x1b[32m------------------Replica: ", conn.RemoteAddr().String(), " ACKed with offset: ", offset, "--------------\x1b[0m")
					m.replicas[conn].offset = offset
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
			for replicaConn, _ := range m.replicas {
				replicaConn.Write(cmd.Serialize())
			}
			m.replicaMu.Unlock()
		}
	}
}

func (m *MasterServer) currentMasterOffset() int64 {
	return m.replOffset
}

func (m *MasterServer) connectednumReplica() int {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()
	return len(m.replicas)
}

func (m *MasterServer) requestReplicaAck() {
	cmd := resp.CreateArray("REPLCONF", "GETACK", "*").Serialize()

	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()

	for replicaConn, _ := range m.replicas {
		replicaConn.Write(cmd)
	}
}

func (m *MasterServer) countReplicasAtOffset(offset int64) int {
	m.replicaMu.Lock()
	defer m.replicaMu.Unlock()

	count := 0
	for _, replica := range m.replicas {
		if replica.offset == offset {
			count++
		}
	}
	return count
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
	case "WAIT":
		conn.Write(m.Wait(args).Serialize())
	default:
		m.RadisServer.handleCommand(conn, command, args)
	}
}
