package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/radis"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this)
var _ = net.Listen
var _ = os.Exit

func main() {
	fmt.Println("Logs from your program will appear here!")
	port := flag.Int("port", 6379, "The port to listen on")
	replicaOf := flag.String("replicaof", "", "The address of the replica to sync with")
	flag.Parse()

	server := radis.NewRadisServer(radis.ServerConfig{
		Address:   fmt.Sprintf("0.0.0.0:%d", *port),
		ReplicaOf: *replicaOf,
	})
	if err := server.Start(); err != nil {
		fmt.Println("Failed to start server: ", err)
		os.Exit(1)
	}

}
