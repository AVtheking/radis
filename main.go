package main

import (
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

	server := radis.NewRadisServer("0.0.0.0:6378")
	if err := server.Start(); err != nil {
		fmt.Println("Failed to start server: ", err)
		os.Exit(1)
	}

}
