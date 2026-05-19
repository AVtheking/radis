package radis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radis/resp"
	"github.com/stretchr/testify/require"
)

// respArray builds a RESP array from strings.
// e.g. respArray("PING") → "*1\r\n$4\r\nPING\r\n"
// e.g. respArray("ECHO", "hello") → "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
func respArray(args ...string) []byte {
	s := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		s += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return []byte(s)
}

// readWithTimeout reads from conn with a deadline so tests don't hang.
func readWithTimeout(t *testing.T, conn net.Conn) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	return string(buf[:n])
}

func startTestServer(t *testing.T) *RadisServer {
	t.Helper()
	server := NewRadisServer(ServerConfig{
		Address:   "127.0.0.1:6378",
		ReplicaOf: "",
	})
	if err := server.Listen(); err != nil {
		t.Fatal("failed to start server:", err)
	}
	t.Cleanup(func() { server.Close() })
	go server.Serve()
	return server
}

func startTestServerWithReplicaOf(t *testing.T) *RadisServer {
	t.Helper()
	server := NewRadisServer(ServerConfig{
		Address:   "127.0.0.1:6377",
		ReplicaOf: "127.0.0.1 6378",
	})
	if err := server.Listen(); err != nil {
		t.Fatal("failed to start server:", err)
	}
	t.Cleanup(func() { server.Close() })
	go server.Serve()
	return server
}

func startTestServerAndConnect(t *testing.T) (net.Conn, error) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	return conn, nil
}

func startTestServerWithReplicaOfAndConnect(t *testing.T) (net.Conn, error) {
	server := startTestServerWithReplicaOf(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	return conn, nil
}

// ==================== PING Tests ====================

func TestHandleConnection(t *testing.T) {
	server := startTestServer(t)

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	_, err = conn.Write(respArray("PING"))
	if err != nil {
		t.Fatal("failed to write:", err)
	}

	got := readWithTimeout(t, conn)
	expected := "+PONG\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestMultiplePings(t *testing.T) {
	server := startTestServer(t)

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 3; i++ {
		conn.Write(respArray("PING"))

		got := readWithTimeout(t, conn)
		if got != "+PONG\r\n" {
			t.Errorf("ping %d: expected +PONG\\r\\n, got %q", i, got)
		}
	}
}

func TestConcurrentClients(t *testing.T) {
	server := startTestServer(t)

	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()

			conn.Write(respArray("PING"))
			got := readWithTimeout(t, conn)
			if got != "+PONG\r\n" {
				errs <- fmt.Errorf("expected +PONG\\r\\n, got %q", got)
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < 5; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// ==================== ECHO Tests ====================

func TestECHOCommnad(t *testing.T) {
	server := startTestServer(t)

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	conn.Write(respArray("ECHO", "hello"))
	got := readWithTimeout(t, conn)
	expected := "$5\r\nhello\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestECHOWrongArgCount(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("ECHO"))
	got := readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error for ECHO with no args, got %q", got)
	}

	conn.Write(respArray("ECHO", "a", "b"))
	got = readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error for ECHO with 2 args, got %q", got)
	}
}

// ==================== RESP Protocol Tests ====================
//
// These tests send commands as RESP arrays (how real Redis clients talk).
// Format: *<num_args>\r\n$<len>\r\n<arg>\r\n ...
//
// Example: PING → *1\r\n$4\r\nPING\r\n

func TestRESP_Ping(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// *1\r\n$4\r\nPING\r\n
	conn.Write(respArray("PING"))
	got := readWithTimeout(t, conn)
	if got != "+PONG\r\n" {
		t.Errorf("expected %q, got %q", "+PONG\r\n", got)
	}
}

func TestRESP_Echo(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
	conn.Write(respArray("ECHO", "hello"))
	got := readWithTimeout(t, conn)
	expected := "$5\r\nhello\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestRESP_EchoWithSpaces(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// RESP bulk strings can carry spaces — this is impossible with inline parsing
	conn.Write(respArray("ECHO", "hello world"))
	got := readWithTimeout(t, conn)
	expected := "$11\r\nhello world\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestRESP_EchoEmptyString(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write(respArray("ECHO", ""))
	got := readWithTimeout(t, conn)
	expected := "$0\r\n\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestRESP_MultiplePingsOnSameConnection(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 5; i++ {
		conn.Write(respArray("PING"))
		got := readWithTimeout(t, conn)
		if got != "+PONG\r\n" {
			t.Errorf("ping %d: expected %q, got %q", i, "+PONG\r\n", got)
		}
	}
}

func TestRESP_UnknownCommand(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write(respArray("FOOBAR"))
	got := readWithTimeout(t, conn)
	// Should respond with a RESP error (starts with -)
	if len(got) == 0 || got[0] != '-' {
		t.Errorf("expected RESP error (starting with '-'), got %q", got)
	}
}

func TestRESP_CommandIsCaseInsensitive(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Redis commands are case-insensitive: "ping", "Ping", "PING" all work
	variants := []string{"ping", "Ping", "pInG", "PING"}
	for _, v := range variants {
		conn.Write(respArray(v))
		got := readWithTimeout(t, conn)
		if got != "+PONG\r\n" {
			t.Errorf("%q: expected %q, got %q", v, "+PONG\r\n", got)
		}
	}
}

func TestRESP_ConcurrentClients(t *testing.T) {
	server := startTestServer(t)

	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()

			conn.Write(respArray("ECHO", "concurrent"))
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				errs <- err
				return
			}
			got := string(buf[:n])
			expected := "$10\r\nconcurrent\r\n"
			if got != expected {
				errs <- fmt.Errorf("expected %q, got %q", expected, got)
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < 5; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// ==================== SET / GET Tests ====================

func TestSETCommand(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value"))
	got := readWithTimeout(t, conn)
	expected := "+OK\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestSETTooFewArgs(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key"))
	got := readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error response, got %q", got)
	}
}

func TestSETOverwritesExistingKey(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "first"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	conn.Write(respArray("SET", "key", "second"))
	got = readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected := "$6\r\nsecond\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestGETCommand(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value"))
	got := readWithTimeout(t, conn)
	expected := "+OK\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected = "$5\r\nvalue\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestGETNonExistentKey(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("GET", "nokey"))
	got := readWithTimeout(t, conn)
	expected := "$-1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestGETWrongArgCount(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("GET"))
	got := readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error for GET with no args, got %q", got)
	}

	conn.Write(respArray("GET", "a", "b"))
	got = readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error for GET with 2 args, got %q", got)
	}
}

func TestSETWithExpiryInSeconds(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value", "EX", "2"))
	got := readWithTimeout(t, conn)
	expected := "+OK\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	//it should return the value here as it is not expired yet
	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected = "$5\r\nvalue\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	time.Sleep(3 * time.Second)
	//it should return -1 here as it is expired now
	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected = "$-1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestSETWithExpiryInMilliseconds(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value", "PX", "2000"))
	got := readWithTimeout(t, conn)
	expected := "+OK\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	//it should return the value here as it is not expired yet
	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected = "$5\r\nvalue\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	time.Sleep(3 * time.Second)
	//it should return -1 here as it is expired now
	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected = "$-1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestSETWithExpiryMissingValue(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value", "EX"))
	got := readWithTimeout(t, conn)
	if got[0] != '-' {
		t.Errorf("expected error response, got %q", got)
	}
}

func TestSETOverwriteRemovesOldExpiry(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("SET", "key", "value", "PX", "500"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	conn.Write(respArray("SET", "key", "newvalue"))
	got = readWithTimeout(t, conn)
	require.Equal(t, "+OK\r\n", got)

	time.Sleep(600 * time.Millisecond)

	conn.Write(respArray("GET", "key"))
	got = readWithTimeout(t, conn)
	expected := "$8\r\nnewvalue\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q (key should not have expired after overwrite)", expected, got)
	}
}

func TestConcurrentSETAndGET(t *testing.T) {
	server := startTestServer(t)

	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()

			key := fmt.Sprintf("key%d", id)
			val := fmt.Sprintf("val%d", id)

			conn.Write(respArray("SET", key, val))
			got := readWithTimeout(t, conn)
			if got != "+OK\r\n" {
				errs <- fmt.Errorf("SET %s: expected +OK, got %q", key, got)
				return
			}

			conn.Write(respArray("GET", key))
			got = readWithTimeout(t, conn)
			expected := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			if got != expected {
				errs <- fmt.Errorf("GET %s: expected %q, got %q", key, expected, got)
				return
			}
			errs <- nil
		}(i)
	}

	for i := 0; i < 10; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// ==================== RPUSH Tests ====================

func TestRPushCommand(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "list", "value1"))
	got := readWithTimeout(t, conn)
	expected := ":1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("RPUSH", "list", "value2"))
	got = readWithTimeout(t, conn)
	expected = ":2\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestRPushMultipleValues(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "list", "value1", "value2", "value3"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

// ==================== LPUSH Tests ====================

func TestLPushCommand(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("LPUSH", "list", "a"))
	got := readWithTimeout(t, conn)
	expected := ":1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LPUSH", "list", "b"))
	got = readWithTimeout(t, conn)
	expected = ":2\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LRange", "list", "0", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$1\r\nb\r\n$1\r\na\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLPushMultipleValues(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("LPUSH", "list", "a", "b", "c"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LRange", "list", "0", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

// ==================== LLEN Tests ====================

func TestLLenCommand(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("LPUSH", "list", "a", "b", "c"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LLEN", "list"))
	got = readWithTimeout(t, conn)
	expected = ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLLenEmptyList(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("LLEN", "list"))
	got := readWithTimeout(t, conn)
	expected := ":0\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

// ==================== LRANGE Tests ====================

func TestLRangeCommand(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "list", "value1", "value2", "value3"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	// conn.Write(respArray("LRange", "list", "0", "-1"))
	// got = readWithTimeout(t, conn)
	// expected = "*3\r\n$5\r\nvalue1\r\n$5\r\nvalue2\r\n$5\r\nvalue3\r\n"
	// if got != expected {
	// 	t.Errorf("expected %q, got %q", expected, got)
	// }

	conn.Write(respArray("LRange", "list", "1", "2"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLRangeNegativeStart(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("RPUSH", "list", "value1", "value2", "value3"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LRange", "list", "-2", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLRangeNegativeEnd(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("RPUSH", "list", "value1", "value2", "value3"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LRange", "list", "0", "-2"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLRangeOutOfBoundsInNegative(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("RPUSH", "list", "value1", "value2", "value3"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LRange", "list", "-100", "-2"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

// ==================== LPOP Tests ====================

func TestLPopCommand(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("RPUSH", "list", "a", "b", "c"))
	got := readWithTimeout(t, conn)
	expected := ":3\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LPOP", "list"))
	got = readWithTimeout(t, conn)
	expected = "$1\r\na\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LRange", "list", "0", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLPopWithNonExistingList(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("LPOP", "list"))
	got := readWithTimeout(t, conn)
	expected := "$-1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
	conn.Write(respArray("LRange", "list", "0", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*-1\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLPoPWithMultipleElements(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("RPUSH", "list", "a", "b", "c", "d", "e"))
	got := readWithTimeout(t, conn)
	expected := ":5\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LPOP", "list", "2"))
	got = readWithTimeout(t, conn)
	expected = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LRange", "list", "0", "-1"))
	got = readWithTimeout(t, conn)
	expected = "*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLPopCountExceedsListLength(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "list", "a", "b"))
	got := readWithTimeout(t, conn)
	require.Equal(t, ":2\r\n", got)

	conn.Write(respArray("LPOP", "list", "5"))
	got = readWithTimeout(t, conn)
	expected := "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}

	conn.Write(respArray("LLEN", "list"))
	got = readWithTimeout(t, conn)
	if got != ":0\r\n" {
		t.Errorf("expected list to be empty, got %q", got)
	}
}

func TestLPopAllElementsOneByone(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "list", "x", "y"))
	readWithTimeout(t, conn)

	conn.Write(respArray("LPOP", "list"))
	got := readWithTimeout(t, conn)
	require.Equal(t, "$1\r\nx\r\n", got)

	conn.Write(respArray("LPOP", "list"))
	got = readWithTimeout(t, conn)
	require.Equal(t, "$1\r\ny\r\n", got)

	conn.Write(respArray("LPOP", "list"))
	got = readWithTimeout(t, conn)
	// list exists but is empty — returns empty array
	require.Equal(t, "*0\r\n", got)
}

func TestRPushThenLPopUntilEmpty(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("RPUSH", "q", "a", "b", "c"))
	got := readWithTimeout(t, conn)
	require.Equal(t, ":3\r\n", got)

	for _, expected := range []string{"a", "b", "c"} {
		conn.Write(respArray("LPOP", "q"))
		got = readWithTimeout(t, conn)
		exp := fmt.Sprintf("$1\r\n%s\r\n", expected)
		if got != exp {
			t.Errorf("expected %q, got %q", exp, got)
		}
	}

	conn.Write(respArray("LPOP", "q"))
	got = readWithTimeout(t, conn)
	// list exists but is empty — returns empty array
	require.Equal(t, "*0\r\n", got)
}

// ==================== Replication Tests ====================

func TestInfoCommand(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("INFO", "Replication"))
	got := readWithTimeout(t, conn)
	expected := "$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestReplicaOfCommand(t *testing.T) {
	conn, err := startTestServerWithReplicaOfAndConnect(t)
	defer conn.Close()
	require.NoError(t, err)

	conn.Write(respArray("INFO", "Replication"))
	got := readWithTimeout(t, conn)
	expected := "$10\r\nrole:slave\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestReplicaHandshakeWithMaster(t *testing.T) {
	_ = startTestServer(t)
	replica := startTestServerWithReplicaOf(t)

	err := replica.handshakeWithMaster()
	require.NoError(t, err)
}

func TestPSyncReturnsFullResync(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
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
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "?", "-1"))

	reader := bufio.NewReader(conn)

	// consume the FULLRESYNC response
	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC"))

	// read the RDB transfer: $<len>\r\n<bytes>
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

	// RDB files start with the REDIS magic header
	require.True(t, strings.HasPrefix(string(rdbData), "REDIS"), "RDB should start with REDIS magic, got %q", string(rdbData[:10]))
}

func TestPSyncWrongArgCount(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.Write(respArray("PSYNC", "?"))
	got := readWithTimeout(t, conn)
	require.True(t, got[0] == '-', "expected error, got %q", got)
}

func TestPSyncWithKnownReplId(t *testing.T) {
	conn, err := startTestServerAndConnect(t)
	require.NoError(t, err)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(respArray("PSYNC", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", "0"))

	reader := bufio.NewReader(conn)
	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, resp.SimpleString, val.Type)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC") || strings.HasPrefix(val.Str, "CONTINUE"))
}

func TestFullHandshakeThenRDB(t *testing.T) {
	server := startTestServer(t)
	conn, err := net.Dial("tcp", server.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	// REPLCONF listening-port
	conn.Write(respArray("REPLCONF", "listening-port", "6380"))
	val, err := resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", string(val.Serialize()))

	// REPLCONF capa psync2
	conn.Write(respArray("REPLCONF", "capa", "psync2"))
	val, err = resp.ParseRESP(reader)
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", string(val.Serialize()))

	// PSYNC ? -1
	conn.Write(respArray("PSYNC", "?", "-1"))
	val, err = resp.ParseRESP(reader)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(val.Str, "FULLRESYNC"))

	// RDB transfer
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
