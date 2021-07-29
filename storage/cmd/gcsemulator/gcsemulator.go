// gcsemulator launches the Cloud Storage emulator on the given address.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/fullstorydev/emulators/storage/gcsemu"
)

var (
	host = flag.String("host", "localhost", "the address to bind to on the local machine")
	port = flag.Int("port", 9000, "the port number to bind to on the local machine")
	dir  = flag.String("dir", "", "if set, use persistence in the given directory")
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

	laddr := fmt.Sprintf("%s:%d", *host, *port)
	server, err := gcsemu.NewServer(laddr, opts)
	if err != nil {
		log.Fatalf("failed to start server: %s", err)
	}
	defer server.Close()

	fmt.Printf("Cloud Storage emulator running on %s\n", server.Addr)
	select {}
}
