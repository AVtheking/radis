package radis

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func addrToReplicaOf(addr string) string {
	host, port, _ := net.SplitHostPort(addr)
	return host + " " + port
}

func startReplicaServer(t *testing.T, masterAddr string) *SlaveServer {
	t.Helper()
	server := NewRadisServer(ServerConfig{
		Address:   "127.0.0.1:6377",
		ReplicaOf: addrToReplicaOf(masterAddr),
	})
	replica := server.(*SlaveServer)
	if err := replica.Listen(); err != nil {
		t.Fatal("failed to start replica:", err)
	}
	t.Cleanup(func() { replica.Close() })
	go replica.Serve()
	return replica
}

func startReplicaServerAndConnectToMaster(t *testing.T, masterAddr string) *SlaveServer {
	t.Helper()
	server := NewRadisServer(ServerConfig{
		Address:   "127.0.0.1:6377",
		ReplicaOf: addrToReplicaOf(masterAddr),
	})
	replica := server.(*SlaveServer)
	if err := replica.Listen(); err != nil {
		t.Fatal("failed to start replica:", err)
	}
	t.Cleanup(func() { replica.Close() })
	if err := replica.ConnectToMaster(); err != nil {
		t.Fatal("failed to connect to master:", err)
	}
	go replica.Serve()
	return replica
}

func startReplicaServerAndConnect(t *testing.T, masterAddr string) (net.Conn, *SlaveServer) {
	t.Helper()
	replica := startReplicaServer(t, masterAddr)
	conn, err := net.Dial("tcp", replica.Addr())
	require.NoError(t, err)
	return conn, replica
}

func startReplicaServerAndConnectToMasterAndConnect(t *testing.T, masterAddr string) (net.Conn, *SlaveServer) {
	t.Helper()
	replica := startReplicaServerAndConnectToMaster(t, masterAddr)
	conn, err := net.Dial("tcp", replica.Addr())
	require.NoError(t, err)
	return conn, replica
}

func TestReplicaInfoCommand(t *testing.T) {
	master := startMasterServer(t)
	conn, _ := startReplicaServerAndConnect(t, master.Addr())
	defer conn.Close()

	conn.Write(respArray("INFO", "Replication"))
	got := readWithTimeout(t, conn)
	expected := "$10\r\nrole:slave\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestReplicaHandshakeWithMaster(t *testing.T) {
	master := startMasterServer(t)
	replica := startReplicaServer(t, master.Addr())

	conn, err := replica.handshakeWithMaster()
	require.NoError(t, err)
	defer conn.Close()
	//test the connection returned is a connection to the master server
	conn.Write(respArray("INFO", "Replication"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n", got)
}

func TestReplicaPingCommand(t *testing.T) {
	master := startMasterServer(t)
	conn, _ := startReplicaServerAndConnect(t, master.Addr())
	defer conn.Close()

	conn.Write(respArray("PING"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+PONG\r\n", got)
}

func TestReplicaEchoCommand(t *testing.T) {
	master := startMasterServer(t)
	conn, _ := startReplicaServerAndConnect(t, master.Addr())
	defer conn.Close()

	conn.Write(respArray("ECHO", "hello"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "$5\r\nhello\r\n", got)
}

func TestReplicaSetAndGet(t *testing.T) {
	master := startMasterServer(t)
	conn, _ := startReplicaServerAndConnect(t, master.Addr())
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	require.Equal(t, "$5\r\nvalue\r\n", got)
}
