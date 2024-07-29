package bigtable

import (
	"cloud.google.com/go/bigtable"
	"context"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/fullstorydev/emulators/bigtable/bttest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLocalServer(t *testing.T) {
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	err = validateServer(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
}

func validateServer(srvAddr string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := grpc.NewClient(srvAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer silentClose(conn)

	proj, instance := "proj", "instance"

	adminClient, err := bigtable.NewAdminClient(ctx, proj, instance, option.WithGRPCConn(conn))
	if err != nil {
		return err
	}
	defer silentClose(adminClient)

	if err = adminClient.CreateTable(ctx, "example"); err != nil {
		return err
	}

	if err = adminClient.CreateColumnFamily(ctx, "example", "links"); err != nil {
		return err
	}

	clientConfig := bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}}
	client, err := bigtable.NewClientWithConfig(ctx, proj, instance, clientConfig, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalln(err)
	}
	defer silentClose(client)

	tbl := client.Open("example")

	mut := bigtable.NewMutation()
	mut.Set("links", "golang.org", bigtable.Now(), []byte("Gophers!"))
	if err = tbl.Apply(ctx, "com.google.cloud", mut); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, "com.google.cloud")
	if err != nil {
		return err
	}
	for _, column := range row["links"] {
		if column.Column != "links:golang.org" {
			return fmt.Errorf("response [%s] != [links:golang.org]", column.Column)
		}
		if string(column.Value) != "Gophers!" {
			return fmt.Errorf("response [%s] != [Gophers!]", string(column.Value))
		}
	}

	return nil
}

func silentClose(c io.Closer) {
	_ = c.Close()
}
