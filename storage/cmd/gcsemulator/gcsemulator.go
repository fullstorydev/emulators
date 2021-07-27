// gcsemulator launches the Cloud Storage emulator on the given address.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"

	"github.com/fullstorydev/emulators/storage/gcsemu"
)

var (
	host = flag.String("host", "localhost", "the address to bind to on the local machine")
	port = flag.Int("port", 9000, "the port number to bind to on the local machine")
	dir  = flag.String("dir", "", "if set, use persistence in the given directory")
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
)

func main() {
	flag.Parse()
	opts := gcsemu.Options{
		Verbose: true,
		Log: func(err error, fmt string, args ...interface{}) {
			if err != nil {
				fmt = "ERROR: " + fmt + ": %s"
				args = append(args, err)
			}
			log.Printf(fmt, args...)
		},
	}
	if *dir != "" {
		fmt.Printf("Writing to: %s\n", *dir)
		opts.Store = gcsemu.NewFileStore(*dir)
	}

	gcsEmu := gcsemu.NewGcsEmu(opts)
	mux := http.NewServeMux()
	mux.HandleFunc("/", gcsEmu.Handler)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if true {
			log.Printf("about to method=%s host=%s u=%s", r.Method, r.Host, r.URL)
		}
		mux.ServeHTTP(w, r)
	}))
	addr := fmt.Sprintf("%s:%d", *host, *port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on addr %s: %s", addr, err)
	}
	srv.Listener = l
	srv.Start()

	fmt.Printf("Cloud Storage emulator running on %s\n", srv.URL)
	select {}
}
