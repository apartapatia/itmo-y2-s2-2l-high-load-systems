package main

import (
	"log"
	"net/http"
	"time"

	"github.com/apartapatia/itmo-y2-s2-2l-high-load-systems/internal/sink"
)

const readHeaderTimeout = 5 * time.Second

func main() {
	server := http.Server{Addr: ":8080", Handler: sink.NewHandler(), ReadHeaderTimeout: readHeaderTimeout}
	log.Fatal(server.ListenAndServe())
}
