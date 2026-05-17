package radis

import (
	"fmt"
	"net"
	"testing"
	"time"
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
	server := NewRadisServer("127.0.0.1:0")
	if err := server.Listen(); err != nil {
		t.Fatal("failed to start server:", err)
	}
	t.Cleanup(func() { server.Close() })
	go server.Serve()
	return server
}

func TestHandleConnection(t *testing.T) {
	server := startTestServer(t)

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Fatal("failed to write:", err)
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal("failed to read response:", err)
	}

	got := string(buf[:n])
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
		conn.Write([]byte("PING\r\n"))

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(buf[:n]) != "+PONG\r\n" {
			t.Errorf("ping %d: expected +PONG\\r\\n, got %q", i, string(buf[:n]))
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

			conn.Write([]byte("PING\r\n"))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				errs <- err
				return
			}
			if string(buf[:n]) != "+PONG\r\n" {
				errs <- fmt.Errorf("expected +PONG\\r\\n, got %q", string(buf[:n]))
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

func TestECHOCommnad(t *testing.T) {
	server := startTestServer(t)

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	conn.Write([]byte("ECHO hello\r\n"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal("failed to read response:", err)
	}
	got := string(buf[:n])
	expected := "$5\r\nhello\r\n"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
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
