package radis

import (
	"fmt"
	"net"
	"testing"
)

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
