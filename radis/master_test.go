package radis

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
	"github.com/stretchr/testify/require"
)

func TestMasterInfoCommand(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.Write(respArray("INFO", "Replication"))
	got := readWithTimeout(t, conn)
	expected := "$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestMasterReplConfListeningPort(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.Write(respArray("REPLCONF", "listening-port", "6380"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)
}

func TestMasterReplConfCapa(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.Write(respArray("REPLCONF", "capa", "psync2"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)
}

func TestMasterReplConfWrongArgCount(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.Write(respArray("REPLCONF"))
	got := readWithTimeout(t, conn)
	require.True(t, got[0] == '-', "expected error, got %q", got)
}

func TestPSyncReturnsFullResync(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "?", "-1"))

	reader := bufio.NewReader(conn)

	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, resp.SimpleString, val.Type)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC"))

	parts := strings.Split(val.Str, " ")
	require.Equal(t, 3, len(parts))
	require.Equal(t, "FULLRESYNC", parts[0])
	require.NotEmpty(t, parts[1])
	require.Equal(t, "0", parts[2])
}

func TestPSyncSendsRDBAfterFullResync(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "?", "-1"))

	reader := bufio.NewReader(conn)

	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC"))

	b, err := reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('$'), b)

	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	require.NoError(t, err)
	require.Greater(t, length, 0)

	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(string(rdbData), "REDIS"), "RDB should start with REDIS magic")
}

func TestPSyncWrongArgCount(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.Write(respArray("PSYNC", "?"))
	got := readWithTimeout(t, conn)
	require.True(t, got[0] == '-', "expected error, got %q", got)
}

func TestPSyncWithKnownReplId(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "0"))

	reader := bufio.NewReader(conn)
	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, resp.SimpleString, val.Type)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC") || strings.HasPrefix(val.Str, "CONTINUE"))
}

// ==================== Full Handshake + RDB Tests ====================

func TestFullHandshakeThenRDB(t *testing.T) {
	server := startMasterServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	conn.Write(respArray("REPLCONF", "listening-port", "6380"))
	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", string(val.Serialize()))

	conn.Write(respArray("REPLCONF", "capa", "psync2"))
	val, err = resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", string(val.Serialize()))

	conn.Write(respArray("PSYNC", "?", "-1"))
	val, err = resp.ParseRESP(reader)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC"))

	b, err := reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('$'), b)

	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	require.NoError(t, err)

	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(rdbData), "REDIS"))
}

func TestRDBTransferHasCorrectLength(t *testing.T) {
	conn, _ := startMasterServerAndConnect(t)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "?", "-1"))

	reader := bufio.NewReader(conn)

	_, err := resp.ParseRESP(reader)
	require.NoError(t, err)

	b, err := reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('$'), b)

	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	declaredLen, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	require.NoError(t, err)

	rdbData := make([]byte, declaredLen)
	n, err := io.ReadFull(reader, rdbData)
	require.NoError(t, err)
	require.Equal(t, declaredLen, n)
}

func TestPropogateToReplicas(t *testing.T) {
	conn, master := startMasterServerAndConnect(t)
	defer conn.Close()

	replicaConn, _ := startReplicaServerAndConnectToMasterAndConnect(t, master.Addr(), "127.0.0.1:6377")
	defer replicaConn.Close()

	conn.Write(respArray("SET", "key", "value"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	time.Sleep(100 * time.Millisecond)

	//check if the replica has received the propogated command from master
	replicaConn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, replicaConn)
	require.Equal(t, "$5\r\nvalue\r\n", got)
}

func TestPropogateToReplicasWithMultipleReplicas(t *testing.T) {
	conn, master := startMasterServerAndConnect(t)
	defer conn.Close()

	replicaConn1, _ := startReplicaServerAndConnectToMasterAndConnect(t, master.Addr(), "127.0.0.1:6377")
	defer replicaConn1.Close()

	replicaConn2, _ := startReplicaServerAndConnectToMasterAndConnect(t, master.Addr(), "127.0.0.1:6376")
	defer replicaConn2.Close()

	conn.Write(respArray("SET", "key", "value"))
	time.Sleep(100 * time.Millisecond)

	replicaConn1.Write(respArray("GET", "key"))
	got := readWithTimeout(t, replicaConn1)
	require.Equal(t, "$5\r\nvalue\r\n", got)

	replicaConn2.Write(respArray("GET", "key"))
	got = readWithTimeout(t, replicaConn2)
	require.Equal(t, "$5\r\nvalue\r\n", got)
}
