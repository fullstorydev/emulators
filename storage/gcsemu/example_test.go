/*
Copyright 2016 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package gcsemu_test

import (
	"context"
	"fmt"
	"github.com/fullstorydev/emulators/storage/gcsemu"
	"github.com/testcontainers/testcontainers-go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
)

func TestExampleLocalServer(t *testing.T) {
	srv, err := gcsemu.NewServer("127.0.0.1:0", gcsemu.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer srv.Close()

	validateServer(srv.Addr)
}


func TestExampleContainerServer(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "gcsemulator",
		ExposedPorts: []string{"9000"},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer c.Terminate(ctx)

	ip, err := c.Host(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	port, err := c.MappedPort(ctx, "9000")
	if err != nil {
		log.Fatalln(err)
	}

	srvAddr := fmt.Sprintf("%s:%s", ip, port.Port())
	fmt.Printf("Big table container started on %s\n", srvAddr)

	validateServer(srvAddr)
}

func validateServer(srvAddr string) {
	// gcsemu.NewClient will look at this env var to figure out what host/port to talk to
	os.Setenv("GCS_EMULATOR_HOST", srvAddr)

	ctx := context.Background()
	fileContent := "FullStory\n" +
		"Google Could Storage Emulator\n" +
		"Gophers!\n"

	client, err := gcsemu.NewClient(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	o := client.Bucket("test").Object("data/test.txt")
	writer := o.NewWriter(ctx)

	_, err = io.Copy(writer, strings.NewReader(fileContent))
	if err != nil {
		log.Fatalln(err)
	}
	err = writer.Close()
	if err != nil {
		log.Fatalln(err)
	}

	reader, err := o.NewReader(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	res, err := ioutil.ReadAll(reader)

	fmt.Printf("%s", string(res))

	// Output:
	// FullStory
	// Google Could Storage Emulator
	// Gophers!
}
