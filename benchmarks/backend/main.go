// Command backend is a minimal HTTP echo server used by the benchmark
// harness. Returns 200 with a fixed-size body for non-/healthz paths,
// 200 for /healthz, and exposes a /_ctl?status=NNN endpoint to flip the
// response status for failure-injection scenarios.
package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
)

var body = make([]byte, 256)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "listen address")
	flag.Parse()

	for i := range body {
		body[i] = 'x'
	}

	var status atomic.Int32
	status.Store(200)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})
	mux.HandleFunc("/_ctl", func(w http.ResponseWriter, r *http.Request) {
		if s := r.URL.Query().Get("status"); s != "" {
			if v, err := strconv.Atoi(s); err == nil {
				status.Store(int32(v))
				w.WriteHeader(200)
				return
			}
		}
		w.WriteHeader(400)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write(body)
	})

	log.Printf("backend listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
