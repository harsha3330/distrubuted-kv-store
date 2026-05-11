package main

import (
	"flag"
	"log"

	httpd "github.com/harsha3330/distubuted-kv-store/http"
)

type Config struct {
	httpAddr string
}

var config Config

const (
	DefaultHTTPAddr = "localhost:11000"
	DefaultRaftAddr = "localhost:12000"
)

func init() {
	flag.StringVar(&config.httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
}

func main() {
	flag.Parse()

	server := httpd.NewServer(config.httpAddr)
	log.Fatal(server.Start())
}
