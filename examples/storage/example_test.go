package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/fullstorydev/emulators/storage/gcsemu"
)

func TestLocalServer(t *testing.T) {
	srv, err := gcsemu.NewServer("localhost:0", gcsemu.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	err = validateServer(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
}

func TestContainerServer(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "fullstorydev/gcsemulator:latest",
		ExposedPorts: []string{"9000"},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Terminate(ctx)

	ip, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "9000")
	if err != nil {
		t.Fatal(err)
	}

	srvAddr := fmt.Sprintf("%s:%s", ip, port.Port())
	fmt.Printf("Storage container started on %s\n", srvAddr)

	err = validateServer(srvAddr)
	if err != nil {
		t.Fatal(err)
	}
}

func validateServer(srvAddr string) error {
	// gcsemu.NewClient will look at this env var to figure out what host/port to talk to
	os.Setenv("GCS_EMULATOR_HOST", srvAddr)

	ctx := context.Background()
	fileContent := "Fullstory\n" +
		"Google Cloud Storage Emulator\n" +
		"Gophers!\n"

	client, err := gcsemu.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	o := client.Bucket("test").Object("data/test.txt")
	writer := o.NewWriter(ctx)

	_, err = io.Copy(writer, strings.NewReader(fileContent))
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}

	reader, err := o.NewReader(ctx)
	if err != nil {
		return err
	}

	res, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	if string(res) != fileContent {
		return fmt.Errorf("response [%s] != file content [%s]", string(res), fileContent)
	}

	return nil
}
