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
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"testing"
)

func TestContainer(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "cbtemulator",
		ExposedPorts: []string{"9000"},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("could not create container. %v", err)
	}
	defer c.Terminate(ctx)

	ip, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("could not get container host. %v", err)
	}
	port, err := c.MappedPort(ctx, "9000")
	if err != nil {
		t.Fatalf("could not map port 9000 on container. %v", err)
	}

	fmt.Printf("Big table container started on %s:%s\n", ip, port.Port())

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", ip, port.Port()), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("could not create connection. %v", err)
	}
	defer conn.Close() // only if the life cycle is scoped to this call

	adminClient, err := bigtable.NewAdminClient(ctx, "TestProject", "TestInstance", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("could not create admin client. %v", err)
	}
	err = adminClient.CreateTable(ctx, "TestTable")
	if err != nil {
		t.Fatalf("could not create table. %v", err)
	}
	err = adminClient.CreateColumnFamily(ctx, "TestTable", "TestFamily")
	if err != nil {
		t.Fatalf("could not create family. %v", err)
	}

	client, err := bigtable.NewClient(ctx, "TestProject", "TestInstance", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("could not create client. %v", err)
	}

	tbl := client.Open("TestTable")
	if err != nil {
		t.Fatalf("could not get table. %v", err)
	}

	mut := bigtable.NewMutation()
	mut.Set("TestFamily", "TestColumn", bigtable.Now(), []byte("TestValue"))
	err = tbl.Apply(ctx, "TestRow", mut)
	if err != nil {
		t.Fatalf("could not apply row mutation. %v", err)
	}

	row, err := tbl.ReadRow(ctx, "TestRow")
	if err != nil {
		t.Fatalf("could not get table info. %v", err)
	}

	for family, data := range row {
		fmt.Printf("family=%s\n", family)
		for _, d := range data {
			fmt.Printf("row=%s, column=%s, value=%s\n", d.Row, d.Column, string(d.Value))
		}
	}
}
