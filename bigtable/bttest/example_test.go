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
package bttest_test

import (
	"cloud.google.com/go/bigtable"
	"context"
	"fmt"
	"github.com/fullstorydev/emulators/bigtable/bttest"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"log"
	"testing"
)

func TestExampleLocalServer(t *testing.T) {
	srv, err := bttest.NewServer("localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	err = validateServer(srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
}

func TestExampleContainerServer(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "fullstorydev/cbtemulator:latest",
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
	fmt.Printf("Big table container started on %s\n", srvAddr)

	err = validateServer(srvAddr)
	if err != nil {
		t.Fatal(err)
	}
}

func validateServer(srvAddr string) error {
	ctx := context.Background()

	conn, err := grpc.Dial(srvAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	proj, instance := "proj", "instance"

	adminClient, err := bigtable.NewAdminClient(ctx, proj, instance, option.WithGRPCConn(conn))
	if err != nil {
		return err
	}

	if err = adminClient.CreateTable(ctx, "example"); err != nil {
		return err
	}

	if err = adminClient.CreateColumnFamily(ctx, "example", "links"); err != nil {
		return err
	}

	client, err := bigtable.NewClient(ctx, proj, instance, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalln(err)
	}
	tbl := client.Open("example")

	mut := bigtable.NewMutation()
	mut.Set("links", "golang.org", bigtable.Now(), []byte("Gophers!"))
	if err = tbl.Apply(ctx, "com.google.cloud", mut); err != nil {
		return err
	}

	if row, err := tbl.ReadRow(ctx, "com.google.cloud"); err != nil {
		return err
	} else {
		for _, column := range row["links"] {
			if column.Column != "links:golang.org" {
				return fmt.Errorf("response [%s] != [links:golang.org]", column.Column)
			}
			if string(column.Value) != "Gophers!" {
				return fmt.Errorf("response [%s] != [Gophers!]", string(column.Value))
			}
		}
	}

	return nil
}
