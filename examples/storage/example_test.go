package storage

import (
	"context"
	"fmt"
	"io"
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

func validateServer(srvAddr string) error {
	// gcsemu.NewClient will look at this env var to figure out what host/port to talk to
	_ = os.Setenv("GCS_EMULATOR_HOST", srvAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fileContent := "Fullstory\n" +
		"Google Cloud Storage Emulator\n" +
		"Gophers!\n"

	client, err := gcsemu.NewClient(ctx)
	if err != nil {
		return err
	}
	defer silentClose(client)

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

	res, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	if string(res) != fileContent {
		return fmt.Errorf("response [%s] != file content [%s]", string(res), fileContent)
	}

	return nil
}

func silentClose(c io.Closer) {
	_ = c.Close()
}
