// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bttest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/sync/errgroup"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type clientIntf struct {
	parent  string
	name    string
	tblName string
	btpb.BigtableClient
	btapb.BigtableTableAdminClient
}

func TestConcurrentMutationsReadModify(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		if _, err := s.CreateTable(
			ctx,
			&btapb.CreateTableRequest{Parent: s.parent, TableId: s.name}); err != nil {
			t.Fatal(err)
		}
	}
	req := &btapb.ModifyColumnFamiliesRequest{
		Name: s.tblName,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
		}},
	}
	_, err := s.ModifyColumnFamilies(ctx, req)
	if err != nil {
		if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
			t.Fatal(err)
		}
	}
	req = &btapb.ModifyColumnFamiliesRequest{
		Name: s.tblName,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id: "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Update{Update: &btapb.ColumnFamily{
				GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}},
			}},
		}},
	}
	if _, err := s.ModifyColumnFamilies(ctx, req); err != nil {
		if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
			t.Fatal(err)
		}
	}

	var ts int64
	ms := func() []*btpb.Mutation {
		return []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte(`col`),
				TimestampMicros: atomic.AddInt64(&ts, 1000),
			}},
		}}
	}

	rmw := func() *btpb.ReadModifyWriteRowRequest {
		return &btpb.ReadModifyWriteRowRequest{
			TableName: s.tblName,
			RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
			Rules: []*btpb.ReadModifyWriteRule{{
				FamilyName:      "cf",
				ColumnQualifier: []byte("col"),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1},
			}},
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		g.Go(func() error {
			for ctx.Err() == nil {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
					Mutations: ms(),
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					return err
				}
			}
			return nil
		})

		g.Go(func() error {
			for ctx.Err() == nil {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
					Mutations: ms(),
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					return err
				}
			}
			return nil
		})
		g.Go(func() error {
			for ctx.Err() == nil {
				_, _ = s.ReadModifyWriteRow(ctx, rmw())
			}
			return nil
		})
	}
	done := make(chan struct{})
	go func() {
		err := g.Wait()
		if s, ok := status.FromError(err); ok && s.Code() == codes.DeadlineExceeded {
			// ok
		} else if err == context.Canceled {
			// ok
		} else if err != nil {
			t.Error(err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Concurrent mutations and GCs haven't completed after 1s")
	}
}

func TestCreateTableResponse(t *testing.T) {
	// We need to ensure that invoking CreateTable returns
	// the  ColumnFamilies as well as Granularity.
	// See issue https://github.com/googleapis/google-cloud-go/issues/1512.
	ctx, s, ok := newClient(t)
	if ok {
		return
	}
	got, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name,
		Table: &btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
				"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	want := &btapb.Table{
		Name: s.tblName,
		// If no Granularity was specified, we should get back "MILLIS".
		Granularity: btapb.Table_MILLIS,
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
			"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
		},
	}
	if !proto.Equal(want, got) {
		t.Fatalf("Response mismatch: wanted:%+v, got:%+v", want, got)
	}
}

func TestCreateTableWithFamily(t *testing.T) {
	// The Go client currently doesn't support creating a table with column families
	// in one operation but it is allowed by the API. This must still be supported by the
	// fake server so this test lives here instead of in the main bigtable
	// integration test.
	ctx, s, ok := newClient(t)
	if ok {
		return
	}
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 123}}},
			"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 456}}},
		},
	}
	cTbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	tbl, err := s.GetTable(ctx, &btapb.GetTableRequest{Name: cTbl.Name})
	if err != nil {
		t.Fatalf("Getting table: %v", err)
	}
	cf := tbl.ColumnFamilies["cf1"]
	if cf == nil {
		t.Fatalf("Missing col family cf1")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(123); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
	cf = tbl.ColumnFamilies["cf2"]
	if cf == nil {
		t.Fatalf("Missing col family cf2")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(456); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
}

func TestSampleRowKeys(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}

	// Populate the table
	val := []byte("value")
	rowCount := 1000
	{
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < rowCount; i++ {
			i := i
			g.Go(func() error {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte("row-" + strconv.Itoa(i)),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf",
							ColumnQualifier: []byte("col"),
							TimestampMicros: 1000,
							Value:           val,
						}},
					}},
				}
				_, err := s.MutateRow(ctx, req)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatalf("Populating table: %v", err)
		}
	}

	responses, err := sampleRowKeys(ctx, s, &btpb.SampleRowKeysRequest{TableName: s.tblName})
	if err != nil {
		t.Fatalf("SampleRowKeys error: %v", err)
	}
	if len(responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}

	// Make sure the offset of the final response is the offset of the final row
	got := responses[len(responses)-1].OffsetBytes
	want := int64((rowCount - 1) * len(val))
	if got != want {
		t.Errorf("Invalid offset: got %d, want %d", got, want)
	}
}

func TestTableRowsConcurrent(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}

	// Populate the table
	populate := func() {
		rowCount := 100
		for i := 0; i < rowCount; i++ {
			req := &btpb.MutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row-" + strconv.Itoa(i)),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("col"),
						TimestampMicros: 1000,
						Value:           []byte("value"),
					}},
				}},
			}
			if _, err := s.MutateRow(ctx, req); err != nil {
				t.Fatalf("Populating table: %v", err)
			}
		}
	}

	attempts := 5
	finished := make(chan bool)
	go func() {
		populate()
		for i := 0; i < attempts; i++ {
			_, err := sampleRowKeys(ctx, s, &btpb.SampleRowKeysRequest{TableName: s.tblName})
			if err != nil {
				t.Errorf("SampleRowKeys error: %v", err)
			}
		}
		finished <- true
	}()
	go func() {
		for i := 0; i < attempts; i++ {
			req := &btapb.DropRowRangeRequest{
				Name:   s.tblName,
				Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
			}
			if _, err := s.DropRowRange(ctx, req); err != nil {
				t.Errorf("Dropping all rows: %v", err)
			}
		}
		finished <- true
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for task %d\n", i)
		}
	}
}

func TestModifyColumnFamilies(t *testing.T) {
	ctx, s, ok := newClient(t)
	if err := populateTable(ctx, s, ok); err != nil {
		t.Fatal(err)
	}

	readRows := func(expectChunks, expectCols, expectFams int) {
		t.Helper()
		responses, err := readRows(ctx, s, &btpb.ReadRowsRequest{TableName: s.tblName})
		if err != nil {
			t.Fatalf("ReadRows error: %v", err)
		}
		cols := map[string]bool{}
		fams := map[string]bool{}
		chunks := 0
		for _, r := range responses {
			for _, c := range r.Chunks {
				chunks++
				colName := c.FamilyName.Value + "." + string(c.Qualifier.Value)
				cols[colName] = true
				fams[c.FamilyName.Value] = true
			}
		}
		if got, want := len(fams), expectFams; got != want {
			t.Errorf("col count: got %d, want %d", got, want)
		}
		if got, want := len(cols), expectCols; got != want {
			t.Errorf("col count: got %d, want %d", got, want)
		}
		if got, want := chunks, expectChunks; got != want {
			t.Errorf("chunk count: got %d, want %d", got, want)
		}
	}

	readRows(27, 9, 3)

	// Now drop the middle column.
	if _, err := s.ModifyColumnFamilies(ctx, &btapb.ModifyColumnFamiliesRequest{
		Name: s.tblName,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf1",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Drop{Drop: true},
		}},
	}); err != nil {
		t.Fatalf("ModifyColumnFamilies error: %v", err)
	}

	readRows(18, 6, 2)

	// adding the column back should not re-create the data.
	if _, err := s.ModifyColumnFamilies(ctx, &btapb.ModifyColumnFamiliesRequest{
		Name: s.tblName,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf1",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
		}},
	}); err != nil {
		t.Fatalf("ModifyColumnFamilies error: %v", err)
	}

	readRows(18, 6, 2)
}

func TestDropRowRange(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	// Populate the table
	prefixes := []string{"AAA", "BBB", "CCC", "DDD"}
	count := 3
	doWrite := func() {
		for _, prefix := range prefixes {
			for i := 0; i < count; i++ {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte(prefix + strconv.Itoa(i)),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf",
							ColumnQualifier: []byte("col"),
							TimestampMicros: 1000,
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					t.Fatalf("Populating table: %v", err)
				}
			}
		}
	}

	numRows := func() int {
		responses, err := readRows(ctx, s, &btpb.ReadRowsRequest{TableName: s.tblName})
		if err != nil {
			t.Fatalf("ReadRows error: %v", err)
		}
		rows := map[string]bool{}
		for _, rsp := range responses {
			for _, c := range rsp.Chunks {
				rows[string(c.RowKey)] = true
			}
		}
		return len(rows)
	}

	doWrite()
	tblSize := numRows()
	req := &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("AAA")},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping first range: %v", err)
	}
	got, want := numRows(), tblSize-count
	if got != want {
		t.Errorf("Row count after first drop: got %d, want %d", got, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("DDD")},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping second range: %v", err)
	}
	got, want = numRows(), tblSize-(2*count)
	if got != want {
		t.Errorf("Row count after second drop: got %d, want %d", got, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("XXX")},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping invalid range: %v", err)
	}
	got, want = numRows(), tblSize-(2*count)
	if got != want {
		t.Errorf("Row count after invalid drop: got %d, want %d", got, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping all data: %v", err)
	}
	got, want = numRows(), 0
	if got != want {
		t.Errorf("Row count after drop all: got %d, want %d", got, want)
	}

	// Test that we can write rows, delete some and then write them again.
	count = 1
	doWrite()

	req = &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping all data: %v", err)
	}
	got, want = numRows(), 0
	if got != want {
		t.Errorf("Row count after drop all: got %d, want %d", got, want)
	}

	doWrite()
	got, want = numRows(), len(prefixes)
	if got != want {
		t.Errorf("Row count after rewrite: got %d, want %d", got, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:   s.tblName,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{RowKeyPrefix: []byte("BBB")},
	}
	if _, err := s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping range: %v", err)
	}
	doWrite()
	got, want = numRows(), len(prefixes)
	if got != want {
		t.Errorf("Row count after drop range: got %d, want %d", got, want)
	}
}

func TestCheckTimestampMaxValue(t *testing.T) {
	// Test that max Timestamp value can be passed in TimestampMicros without error
	// and that max Timestamp is the largest valid value in Millis.
	// See issue https://github.com/googleapis/google-cloud-go/issues/1790
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	var maxTimestamp int64 = math.MaxInt64 - math.MaxInt64%1000
	mreq1 := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: maxTimestamp,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq1); err != nil {
		t.Fatalf("TimestampMicros wasn't set: %v", err)
	}

	mreq2 := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: maxTimestamp + 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq2); err == nil {
		t.Fatalf("want TimestampMicros rejection, got acceptance: %v", err)
	}
}

func TestReadRows(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	mreq := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	for _, rowset := range []*btpb.RowSet{
		{RowKeys: [][]byte{[]byte("row")}},
		{RowRanges: []*btpb.RowRange{{StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("")}}}},
		{RowRanges: []*btpb.RowRange{{StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("r")}}}},
		{RowRanges: []*btpb.RowRange{{
			StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("")},
			EndKey:   &btpb.RowRange_EndKeyOpen{EndKeyOpen: []byte("s")},
		}}},
	} {
		responses, err := readRows(ctx, s, &btpb.ReadRowsRequest{TableName: s.tblName})
		if err != nil {
			t.Fatalf("ReadRows error: %v", err)
		}
		if got, want := len(responses), 1; got != want {
			t.Errorf("%+v: response count: got %d, want %d", rowset, got, want)
		}
	}
}

func TestReadRowsError(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	mreq := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	req := &btpb.ReadRowsRequest{TableName: s.tblName, Filter: &btpb.RowFilter{
		Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("[")}}, // Invalid regex.
	}
	if _, err := readRows(ctx, s, req); err == nil {
		t.Fatal("ReadRows got no error, want error")
	}
}

func TestReadRowsAfterDeletion(t *testing.T) {
	ctx, s, ok := newClient(t)
	if err := populateTable(ctx, s, ok); err != nil {
		t.Fatal(err)
	}
	dreq := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_DeleteFromRow_{
				DeleteFromRow: &btpb.Mutation_DeleteFromRow{},
			},
		}},
	}
	if _, err := s.MutateRow(ctx, dreq); err != nil {
		t.Fatalf("Deleting from table: %v", err)
	}

	responses, err := readRows(ctx, s, &btpb.ReadRowsRequest{TableName: s.tblName})
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if got, want := len(responses), 0; got != want {
		t.Errorf("response count: got %d, want %d", got, want)
	}
}

func TestReadRowsOrder(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	count := 3
	mcf := func(i int) *btapb.ModifyColumnFamiliesRequest {
		return &btapb.ModifyColumnFamiliesRequest{
			Name: s.tblName,
			Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
				Id:  "cf" + strconv.Itoa(i),
				Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
			}},
		}
	}
	for i := 1; i <= count; i++ {
		_, err := s.ModifyColumnFamilies(ctx, mcf(i))
		if err != nil {
			if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
				t.Fatal(err)
			}
		}
	}
	// Populate the table
	for fc := 0; fc < count; fc++ {
		for cc := count; cc > 0; cc-- {
			for tc := 0; tc < count; tc++ {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte("row"),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf" + strconv.Itoa(fc),
							ColumnQualifier: []byte("col" + strconv.Itoa(cc)),
							TimestampMicros: int64((tc + 1) * 1000),
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					t.Fatalf("Populating table: %v", err)
				}
			}
		}
	}
	req := &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}
	responses, err := readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}

	if got, want := len(responses[0].Chunks), 27; got != want {
		t.Errorf("Chunk count: got %d, want %d", got, want)
	}
	testOrder := func(responses []*btpb.ReadRowsResponse) {
		var prevFam string
		var prevCol []byte
		var prevTime int64
		for _, cc := range responses[0].Chunks {
			if prevFam == "" {
				prevFam = cc.FamilyName.Value
				prevCol = cc.Qualifier.Value
				prevTime = cc.TimestampMicros
				continue
			}
			if cc.FamilyName.Value < prevFam {
				t.Errorf("Family order is not correct: got %s < %s", cc.FamilyName.Value, prevFam)
			} else if cc.FamilyName.Value == prevFam {
				if cmp := bytes.Compare(cc.Qualifier.Value, prevCol); cmp < 0 {
					t.Errorf("Column order is not correct: got %s < %s", cc.Qualifier.Value, prevCol)
				} else if cmp == 0 {
					if cc.TimestampMicros > prevTime {
						t.Errorf("cell order is not correct: got %d > %d", cc.TimestampMicros, prevTime)
					}
				}
			}
			prevFam = cc.FamilyName.Value
			prevCol = cc.Qualifier.Value
			prevTime = cc.TimestampMicros
		}
	}
	testOrder(responses)

	// Read with interleave filter
	inter := &btpb.RowFilter_Interleave{}
	fnr := &btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "cf1"}}
	cqr := &btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("col2")}}
	inter.Filters = append(inter.Filters, fnr, cqr)
	req = &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_Interleave_{Interleave: inter},
		},
	}

	responses, err = readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if len(responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}

	// cf1 12 cells will be returned (3 are dups)
	// cf0 and cf2 the 3 cells in col2 will be returned
	if got, want := len(responses[0].Chunks), 3+12+3; got != want {
		t.Errorf("Chunk count: got %d, want %d", got, want)
	}
	testOrder(responses)

	// Check order after ReadModifyWriteRow
	rmw := func(i int) *btpb.ReadModifyWriteRowRequest {
		return &btpb.ReadModifyWriteRowRequest{
			TableName: s.tblName,
			RowKey:    []byte("row"),
			Rules: []*btpb.ReadModifyWriteRule{{
				FamilyName:      "cf3",
				ColumnQualifier: []byte("col" + strconv.Itoa(i)),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1},
			}},
		}
	}
	for i := count; i > 0; i-- {
		if _, err := s.ReadModifyWriteRow(ctx, rmw(i)); err != nil {
			t.Fatal(err)
		}
	}
	req = &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}
	responses, err = readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if len(responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}

	// cf0, cf1, and cf2 all 9 cells will be returned
	// cf3 all 3 cells will be returned
	if got, want := len(responses[0].Chunks), 9+9+9+3; got != want {
		t.Errorf("Chunk count: got %d, want %d", got, want)
	}
	testOrder(responses)
}

func TestReadRowsWithlabelTransformer(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}
	mreq := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf0",
				ColumnQualifier: []byte("col"),
				TimestampMicros: 1000,
				Value:           []byte{},
			}},
		}},
	}
	if _, err := s.MutateRow(ctx, mreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	req := &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ApplyLabelTransformer{
				ApplyLabelTransformer: "label",
			},
		},
	}
	responses, err := readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if got, want := len(responses), 1; got != want {
		t.Fatalf("response count: got %d, want %d", got, want)
	}
	resp := responses[0]
	if got, want := len(resp.Chunks), 1; got != want {
		t.Fatalf("chunks count: got %d, want %d", got, want)
	}
	chunk := resp.Chunks[0]
	if got, want := len(chunk.Labels), 1; got != want {
		t.Fatalf("labels count: got %d, want %d", got, want)
	}
	if got, want := chunk.Labels[0], "label"; got != want {
		t.Fatalf("label: got %s, want %s", got, want)
	}

	req = &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ApplyLabelTransformer{
				ApplyLabelTransformer: "", // invalid label
			},
		},
	}
	if _, err := readRows(ctx, s, req); err == nil {
		t.Fatal("ReadRows want invalid label error, got none")
	}
}

func TestCheckAndMutateRowWithoutPredicate(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}

	val := []byte("value")
	muts := []*btpb.Mutation{{
		Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
			FamilyName:      "cf",
			ColumnQualifier: []byte("col"),
			TimestampMicros: 1000,
			Value:           val,
		}},
	}}

	// Populate the table
	mrreq := &btpb.MutateRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row-present"),
		Mutations: muts,
	}
	if _, err := s.MutateRow(ctx, mrreq); err != nil {
		t.Fatalf("Populating table: %v", err)
	}

	req := &btpb.CheckAndMutateRowRequest{
		TableName:      s.tblName,
		RowKey:         []byte("row-not-present"),
		FalseMutations: muts,
	}
	if res, err := s.CheckAndMutateRow(ctx, req); err != nil {
		t.Errorf("CheckAndMutateRow error: %v", err)
	} else if got, want := res.PredicateMatched, false; got != want {
		t.Errorf("Invalid PredicateMatched value: got %t, want %t", got, want)
	}

	req = &btpb.CheckAndMutateRowRequest{
		TableName:      s.tblName,
		RowKey:         []byte("row-present"),
		FalseMutations: muts,
	}
	if res, err := s.CheckAndMutateRow(ctx, req); err != nil {
		t.Errorf("CheckAndMutateRow error: %v", err)
	} else if got, want := res.PredicateMatched, true; got != want {
		t.Errorf("Invalid PredicateMatched value: got %t, want %t", got, want)
	}
}

func TestCheckAndMutateRowWithPredicate(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		tblReq := &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name,
			Table: &btapb.Table{
				ColumnFamilies: map[string]*btapb.ColumnFamily{
					"cf": {},
					"df": {},
					"ef": {},
					"ff": {},
					"zf": {},
				},
			},
		}
		_, err := s.CreateTable(ctx, tblReq)
		if err != nil {
			t.Fatalf("Failed to create the table: %v", err)
		}
	}

	entries := []struct {
		row                         string
		value                       []byte
		familyName, columnQualifier string
	}{
		{"row1", []byte{0x11}, "cf", "cq"},
		{"row2", []byte{0x1a}, "df", "dq"},
		{"row3", []byte{'a'}, "ef", "eq"},
		{"row4", []byte{'b'}, "ff", "fq"},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: s.tblName,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      entry.familyName,
					ColumnQualifier: []byte(entry.columnQualifier),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	var bogusMutations = []*btpb.Mutation{{
		Mutation: &btpb.Mutation_DeleteFromFamily_{
			DeleteFromFamily: &btpb.Mutation_DeleteFromFamily{
				FamilyName: "bogus_family",
			},
		},
	}}

	tests := []struct {
		req       *btpb.CheckAndMutateRowRequest
		wantMatch bool
		name      string

		// if wantState is nil, that means we don't care to check
		// what the state of the world is.
		wantState []*btpb.ReadRowsResponse_CellChunk
	}{
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_RowKeyRegexFilter{
						RowKeyRegexFilter: []byte("not-one"),
					},
				},
				TrueMutations: bogusMutations,
			},
			name: "no match",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_RowKeyRegexFilter{
						RowKeyRegexFilter: []byte("ro.+"),
					},
				},
				FalseMutations: bogusMutations,
			},
			wantMatch: true,
			name:      "rowkey regex",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_PassAllFilter{
						PassAllFilter: true,
					},
				},
				FalseMutations: bogusMutations,
			},
			wantMatch: true,
			name:      "pass all",
		},
		{
			req: &btpb.CheckAndMutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row1"),
				PredicateFilter: &btpb.RowFilter{
					Filter: &btpb.RowFilter_BlockAllFilter{
						BlockAllFilter: true,
					},
				},
				FalseMutations: []*btpb.Mutation{
					{
						Mutation: &btpb.Mutation_SetCell_{
							SetCell: &btpb.Mutation_SetCell{
								FamilyName:      "zf",
								Value:           []byte("foo"),
								TimestampMicros: 2000,
								ColumnQualifier: []byte("et"),
							},
						},
					},
				},
			},
			name:      "BlockAll for row1",
			wantMatch: false,
			wantState: []*btpb.ReadRowsResponse_CellChunk{
				{
					RowKey: []byte("row1"),
					FamilyName: &wrappers.StringValue{
						Value: "cf",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("cq"),
					},
					TimestampMicros: 1000,
					Value:           []byte{0x11},
				},
				{
					RowKey: []byte("row1"),
					FamilyName: &wrappers.StringValue{
						Value: "zf",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("et"),
					},
					TimestampMicros: 2000,
					Value:           []byte("foo"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row2"),
					FamilyName: &wrappers.StringValue{
						Value: "df",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("dq"),
					},
					TimestampMicros: 1000,
					Value:           []byte{0x1a},
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row3"),
					FamilyName: &wrappers.StringValue{
						Value: "ef",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("eq"),
					},
					TimestampMicros: 1000,
					Value:           []byte("a"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
				{
					RowKey: []byte("row4"),
					FamilyName: &wrappers.StringValue{
						Value: "ff",
					},
					Qualifier: &wrappers.BytesValue{
						Value: []byte("fq"),
					},
					TimestampMicros: 1000,
					Value:           []byte("b"),
					RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
						CommitRow: true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := s.CheckAndMutateRow(ctx, tt.req)
			if err != nil {
				t.Fatalf("CheckAndMutateRow error: %v", err)
			}
			got, want := res.PredicateMatched, tt.wantMatch
			if got != want {
				t.Fatalf("Invalid PredicateMatched value: got %t, want %t\nRequest: %+v", got, want, tt.req)
			}

			if tt.wantState == nil {
				return
			}

			rreq := &btpb.ReadRowsRequest{TableName: s.tblName}
			responses, err := readRows(ctx, s, rreq)
			if err != nil {
				t.Fatalf("ReadRows error: %v", err)
			}

			// Collect all the cellChunks
			var gotCellChunks []*btpb.ReadRowsResponse_CellChunk
			for _, res := range responses {
				gotCellChunks = append(gotCellChunks, res.Chunks...)
			}
			sort.Slice(gotCellChunks, func(i, j int) bool {
				ci, cj := gotCellChunks[i], gotCellChunks[j]
				return compareCellChunks(ci, cj)
			})
			wantCellChunks := tt.wantState[0:]
			sort.Slice(wantCellChunks, func(i, j int) bool {
				return compareCellChunks(wantCellChunks[i], wantCellChunks[j])
			})

			// bttest for some reason undeterministically returns:
			//      RowStatus: &bigtable.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true},
			// so we'll ignore that field during comparison.
			scrubRowStatus := func(cs []*btpb.ReadRowsResponse_CellChunk) []*btpb.ReadRowsResponse_CellChunk {
				for _, c := range cs {
					c.RowStatus = nil
				}
				return cs
			}
			diff := cmp.Diff(scrubRowStatus(gotCellChunks), scrubRowStatus(wantCellChunks), cmp.Comparer(proto.Equal))
			if diff != "" {
				t.Fatalf("unexpected response: %s", diff)
			}
		})
	}
}

// compareCellChunks is a comparator that is passed
// into sort.Slice to stably sort cell chunks.
func compareCellChunks(ci, cj *btpb.ReadRowsResponse_CellChunk) bool {
	if bytes.Compare(ci.RowKey, cj.RowKey) > 0 {
		return false
	}
	if bytes.Compare(ci.Value, cj.Value) > 0 {
		return false
	}
	return ci.FamilyName.GetValue() < cj.FamilyName.GetValue()
}

func TestServer_ReadModifyWriteRow(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			t.Fatalf("Creating table: %v", err)
		}
	}

	req := &btpb.ReadModifyWriteRowRequest{
		TableName: s.tblName,
		RowKey:    []byte("row-key"),
		Rules: []*btpb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q1"),
				Rule: &btpb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("a"),
				},
			},
			// multiple ops for same cell
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q1"),
				Rule: &btpb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("b"),
				},
			},
			// different cell whose qualifier should sort before the prior rules
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q0"),
				Rule: &btpb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 1,
				},
			},
		},
	}

	got, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("ReadModifyWriteRow error: %v", err)
	}

	want := &btpb.ReadModifyWriteRowResponse{
		Row: &btpb.Row{
			Key: []byte("row-key"),
			Families: []*btpb.Family{{
				Name: "cf",
				Columns: []*btpb.Column{
					{
						Qualifier: []byte("q0"),
						Cells: []*btpb.Cell{{
							Value: []byte{0, 0, 0, 0, 0, 0, 0, 1},
						}},
					},
					{
						Qualifier: []byte("q1"),
						Cells: []*btpb.Cell{{
							Value: []byte("ab"),
						}},
					},
				},
			}},
		},
	}

	scrubTimestamp := func(resp *btpb.ReadModifyWriteRowResponse) *btpb.ReadModifyWriteRowResponse {
		for _, fam := range resp.GetRow().GetFamilies() {
			for _, col := range fam.GetColumns() {
				for _, cell := range col.GetCells() {
					cell.TimestampMicros = 0
				}
			}
		}
		return resp
	}
	diff := cmp.Diff(scrubTimestamp(got), scrubTimestamp(want), cmp.Comparer(proto.Equal))
	if diff != "" {
		t.Errorf("unexpected response: %s", diff)
	}
}

// helper function to populate table data
func populateTable(ctx context.Context, s *clientIntf, exists bool) error {
	if !exists {
		newTbl := btapb.Table{
			ColumnFamilies: map[string]*btapb.ColumnFamily{
				"cf0": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{MaxNumVersions: 1}}},
			},
		}
		_, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name, Table: &newTbl})
		if err != nil {
			return err
		}
	}

	count := 3
	mcf := func(i int) *btapb.ModifyColumnFamiliesRequest {
		return &btapb.ModifyColumnFamiliesRequest{
			Name: s.tblName,
			Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
				Id:  "cf" + strconv.Itoa(i),
				Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{Create: &btapb.ColumnFamily{}},
			}},
		}
	}
	for i := 1; i <= count; i++ {
		_, err := s.ModifyColumnFamilies(ctx, mcf(i))
		if err != nil {
			if s, ok := status.FromError(err); !ok || s.Code() != codes.AlreadyExists {
				return err
			}
		}
	}
	// Populate the table
	for fc := 0; fc < count; fc++ {
		for cc := count; cc > 0; cc-- {
			for tc := 0; tc < count; tc++ {
				req := &btpb.MutateRowRequest{
					TableName: s.tblName,
					RowKey:    []byte("row"),
					Mutations: []*btpb.Mutation{{
						Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
							FamilyName:      "cf" + strconv.Itoa(fc),
							ColumnQualifier: []byte("col" + strconv.Itoa(cc)),
							TimestampMicros: int64((tc + 1) * 1000),
							Value:           []byte{},
						}},
					}},
				}
				if _, err := s.MutateRow(ctx, req); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func TestFilters(t *testing.T) {
	tests := []struct {
		in   *btpb.RowFilter
		code codes.Code
		out  int
	}{
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_BlockAllFilter{BlockAllFilter: true}}, out: 0},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_BlockAllFilter{BlockAllFilter: false}}, code: codes.InvalidArgument},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}}, out: 1},
		{in: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: false}}, code: codes.InvalidArgument},
	}

	ctx, s, ok := newClient(t)
	if err := populateTable(ctx, s, ok); err != nil {
		t.Fatal(err)
	}

	req := &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte("row")}},
	}

	for _, tc := range tests {
		req.Filter = tc.in

		responses, err := readRows(ctx, s, req)
		if tc.code != codes.OK {
			s, _ := status.FromError(err)
			if s.Code() != tc.code {
				t.Errorf("error code: got %d, want %d", s.Code(), tc.code)
			}
		} else if err != nil {
			t.Errorf("ReadRows error: %v", err)
		} else if len(responses) != tc.out {
			t.Errorf("Response count: got %d, want %d", len(responses), tc.out)
		}
	}
}

func Test_Mutation_DeleteFromColumn(t *testing.T) {
	ctx, s, ok := newClient(t)
	if err := populateTable(ctx, s, ok); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		in   *btpb.MutateRowRequest
		fail bool
	}{
		{
			in: &btpb.MutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf1",
						ColumnQualifier: []byte("col1"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 2000,
							EndTimestampMicros:   1000,
						},
					}},
				}},
			},
			fail: true,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf2",
						ColumnQualifier: []byte("col2"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 1000,
							EndTimestampMicros:   2000,
						},
					}},
				}},
			},
			fail: false,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf3",
						ColumnQualifier: []byte("col3"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 1000,
							EndTimestampMicros:   0,
						},
					}},
				}},
			},
			fail: false,
		},
		{
			in: &btpb.MutateRowRequest{
				TableName: s.tblName,
				RowKey:    []byte("row"),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_DeleteFromColumn_{DeleteFromColumn: &btpb.Mutation_DeleteFromColumn{
						FamilyName:      "cf4",
						ColumnQualifier: []byte("col4"),
						TimeRange: &btpb.TimestampRange{
							StartTimestampMicros: 0,
							EndTimestampMicros:   1000,
						},
					}},
				}},
			},
			fail: true,
		},
	}
	for _, test := range tests {
		_, err := s.MutateRow(ctx, test.in)

		if err != nil && !test.fail {
			t.Errorf("expected passed got failure for : %v \n with err: %v", test.in, err)
		}

		if err == nil && test.fail {
			t.Errorf("expected failure got passed for : %v", test)
		}
	}
}

func TestFilterRow(t *testing.T) {
	row := &btpb.Row{
		Key: []byte("row"),
		Families: []*btpb.Family{
			{
				Name: "fam",
				Columns: []*btpb.Column{
					{
						Qualifier: []byte("col"),
						Cells:     []*btpb.Cell{{TimestampMicros: 1000, Value: []byte("val")}},
					},
				},
			},
		},
	}
	for _, test := range []struct {
		filter *btpb.RowFilter
		want   bool
	}{
		// The regexp-based filters perform whole-string, case-sensitive matches.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("row")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("ro")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("ROW")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "fam"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "f.*"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "[fam]+"}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "fa"}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "FAM"}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "moo"}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("col")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("co")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("COL")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("val")}}, true},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("va")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("VAL")}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("moo")}}, false},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{TimestampRangeFilter: &btpb.TimestampRange{StartTimestampMicros: int64(0), EndTimestampMicros: int64(1000)}}}, false},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{TimestampRangeFilter: &btpb.TimestampRange{StartTimestampMicros: int64(1000), EndTimestampMicros: int64(2000)}}}, true},
	} {
		got, err := filterRow(test.filter, copyRow(row))
		if err != nil {
			t.Errorf("%s: got unexpected error: %v", test.filter, err)
		}
		if got != test.want {
			t.Errorf("%s: got %t, want %t", test.filter, got, test.want)
		}
	}
}

func TestFilterRowWithErrors(t *testing.T) {
	row := &btpb.Row{
		Key: []byte("row"),
		Families: []*btpb.Family{
			{
				Name: "fam",
				Columns: []*btpb.Column{
					{
						Qualifier: []byte("col"),
						Cells:     []*btpb.Cell{{TimestampMicros: 1000, Value: []byte("val")}},
					},
				},
			},
		},
	}
	for _, test := range []struct {
		badRegex *btpb.RowFilter
	}{
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowKeyRegexFilter{RowKeyRegexFilter: []byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "["}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("[")}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
			Chain: &btpb.RowFilter_Chain{Filters: []*btpb.RowFilter{
				{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("[")}}},
			},
		}}},
		{&btpb.RowFilter{Filter: &btpb.RowFilter_Condition_{
			Condition: &btpb.RowFilter_Condition{
				PredicateFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("[")}},
			},
		}}},

		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{RowSampleFilter: 0.0}}},                                                                                             // 0.0 is invalid.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{RowSampleFilter: 1.0}}},                                                                                             // 1.0 is invalid.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{TimestampRangeFilter: &btpb.TimestampRange{StartTimestampMicros: int64(1), EndTimestampMicros: int64(1000)}}}}, // Server only supports millisecond precision.
		{&btpb.RowFilter{Filter: &btpb.RowFilter_TimestampRangeFilter{TimestampRangeFilter: &btpb.TimestampRange{StartTimestampMicros: int64(1000), EndTimestampMicros: int64(1)}}}}, // Server only supports millisecond precision.
	} {
		got, err := filterRow(test.badRegex, copyRow(row))
		if got != false {
			t.Errorf("%s: got true, want false", test.badRegex)
		}
		if err == nil {
			t.Errorf("%s: got no error, want error", test.badRegex)
		}
	}
}

func TestFilterRowWithRowSampleFilter(t *testing.T) {
	prev := randFloat
	randFloat = func() float64 { return 0.5 }
	defer func() { randFloat = prev }()
	for _, test := range []struct {
		p    float64
		want bool
	}{
		{0.1, false}, // Less than random float. Return no rows.
		{0.5, false}, // Equal to random float. Return no rows.
		{0.9, true},  // Greater than random float. Return all rows.
	} {
		got, err := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_RowSampleFilter{RowSampleFilter: test.p}}, &btpb.Row{})
		if err != nil {
			t.Fatalf("%f: %v", test.p, err)
		}
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.p, got, test.want)
		}
	}
}

func TestFilterRowWithBinaryColumnQualifier(t *testing.T) {
	rs := []byte{128, 128}
	row := &btpb.Row{
		Key: rs,
		Families: []*btpb.Family{
			{
				Name: "fam",
				Columns: []*btpb.Column{
					{
						Qualifier: rs,
						Cells:     []*btpb.Cell{{TimestampMicros: 1000, Value: []byte("val")}},
					},
				},
			},
		},
	}
	for _, test := range []struct {
		filter string
		want   bool
	}{
		{`\x80\x80`, true},      // succeeds, exact match
		{`\x80\x81`, false},     // fails
		{`\x80`, false},         // fails, because the regexp must match the entire input
		{`\x80*`, true},         // succeeds: 0 or more 128s
		{`[\x7f\x80]{2}`, true}, // succeeds: exactly two of either 127 or 128
		{`\C{2}`, true},         // succeeds: two bytes
	} {
		got, _ := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte(test.filter)}}, copyRow(row))
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.filter, got, test.want)
		}
	}
}

func TestFilterRowWithUnicodeColumnQualifier(t *testing.T) {
	rs := []byte("a§b")
	row := &btpb.Row{
		Key: rs,
		Families: []*btpb.Family{
			{
				Name: "fam",
				Columns: []*btpb.Column{
					{
						Qualifier: rs,
						Cells:     []*btpb.Cell{{TimestampMicros: 1000, Value: []byte("val")}},
					},
				},
			},
		},
	}
	for _, test := range []struct {
		filter string
		want   bool
	}{
		{`a§b`, true},        // succeeds, exact match
		{`a\xC2\xA7b`, true}, // succeeds, exact match
		{`a\xC2.+`, true},    // succeeds, prefix match
		{`a\xC2\C{2}`, true}, // succeeds, prefix match
		{`a\xC.+`, false},    // fails, prefix match, bad escape
		{`a§.+`, true},       // succeeds, prefix match
		{`.+§b`, true},       // succeeds, suffix match
		{`.§b`, true},        // succeeds
		{`a§c`, false},       // fails
		{`§b`, false},        // fails, because the regexp must match the entire input
		{`.*§.*`, true},      // succeeds: anything with a §
		{`.+§.+`, true},      // succeeds: anything with a § in the middle
		{`a\C{2}b`, true},    // succeeds: § is two bytes
		{`\C{4}`, true},      // succeeds: four bytes
	} {
		got, _ := filterRow(&btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte(test.filter)}}, copyRow(row))
		if got != test.want {
			t.Errorf("%v: got %t, want %t", test.filter, got, test.want)
		}
	}
}

// Test that a single column qualifier with the interleave filter returns
// the correct result and not return every single row.
// See Issue https://github.com/googleapis/google-cloud-go/issues/1399
func TestFilterRowWithSingleColumnQualifier(t *testing.T) {
	ctx, s, ok := newClient(t)
	if !ok {
		tblReq := &btapb.CreateTableRequest{Parent: s.parent, TableId: s.name,
			Table: &btapb.Table{
				ColumnFamilies: map[string]*btapb.ColumnFamily{
					"cf": {},
				},
			},
		}
		_, err := s.CreateTable(ctx, tblReq)
		if err != nil {
			t.Fatalf("Failed to create the table: %v", err)
		}
	}

	entries := []struct {
		row   string
		value []byte
	}{
		{"row1", []byte{0x11}},
		{"row2", []byte{0x1a}},
		{"row3", []byte{'a'}},
		{"row4", []byte{'b'}},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: s.tblName,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("cq"),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	// After insertion now it is time for querying.
	req := &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Filter: &btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
			Chain: &btpb.RowFilter_Chain{Filters: []*btpb.RowFilter{{
				Filter: &btpb.RowFilter_Interleave_{
					Interleave: &btpb.RowFilter_Interleave{
						Filters: []*btpb.RowFilter{{Filter: &btpb.RowFilter_Condition_{
							Condition: &btpb.RowFilter_Condition{
								PredicateFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{
									Chain: &btpb.RowFilter_Chain{Filters: []*btpb.RowFilter{
										{
											Filter: &btpb.RowFilter_ValueRangeFilter{ValueRangeFilter: &btpb.ValueRange{
												StartValue: &btpb.ValueRange_StartValueClosed{
													StartValueClosed: []byte("a"),
												},
												EndValue: &btpb.ValueRange_EndValueClosed{EndValueClosed: []byte("a")},
											}},
										},
										{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
									}},
								}},
								TrueFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
							},
						}},
							{Filter: &btpb.RowFilter_BlockAllFilter{BlockAllFilter: true}},
						},
					},
				},
			},
				{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
			}},
		}},
	}

	responses, err := readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	if g, w := len(responses), 1; g != w {
		t.Fatalf("Results/Streamed chunks mismatch:: got %d want %d", g, w)
	}

	got := responses[0]
	// Only row3 should be matched.
	want := &btpb.ReadRowsResponse{
		Chunks: []*btpb.ReadRowsResponse_CellChunk{
			{
				RowKey:          []byte("row3"),
				FamilyName:      &wrappers.StringValue{Value: "cf"},
				Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
				TimestampMicros: 1000,
				Value:           []byte("a"),
				RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
					CommitRow: true,
				},
			},
		},
	}
	if diff := cmp.Diff(got, want, cmp.Comparer(proto.Equal)); diff != "" {
		t.Fatalf("Response mismatch: got: + want -\n%s", diff)
	}
}

func TestValueFilterRowWithAlternationInRegex(t *testing.T) {
	// Test that regex alternation is applied properly.
	// See Issue https://github.com/googleapis/google-cloud-go/issues/1499
	ctx, s, ok := newClient(t)
	if !ok {

		tblReq := &btapb.CreateTableRequest{
			Parent:  s.parent,
			TableId: s.name,
			Table: &btapb.Table{
				ColumnFamilies: map[string]*btapb.ColumnFamily{
					"cf": {},
				},
			},
		}
		_, err := s.CreateTable(ctx, tblReq)
		if err != nil {
			t.Fatalf("Failed to create the table: %v", err)
		}
	}

	entries := []struct {
		row   string
		value []byte
	}{
		{"row1", []byte("")},
		{"row2", []byte{'x'}},
		{"row3", []byte{'a'}},
		{"row4", []byte{'m'}},
	}

	for _, entry := range entries {
		req := &btpb.MutateRowRequest{
			TableName: s.tblName,
			RowKey:    []byte(entry.row),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("cq"),
					TimestampMicros: 1000,
					Value:           entry.value,
				}},
			}},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			t.Fatalf("Failed to insert entry %v into server: %v", entry, err)
		}
	}

	// After insertion now it is time for querying.
	req := &btpb.ReadRowsRequest{
		TableName: s.tblName,
		Rows:      &btpb.RowSet{},
		Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_ValueRegexFilter{
				ValueRegexFilter: []byte("|a"),
			},
		},
	}

	responses, err := readRows(ctx, s, req)
	if err != nil {
		t.Fatalf("ReadRows error: %v", err)
	}
	var gotChunks []*btpb.ReadRowsResponse_CellChunk
	for _, res := range responses {
		gotChunks = append(gotChunks, res.Chunks...)
	}

	// Only row1 "" and row3 "a" should be matched.
	wantChunks := []*btpb.ReadRowsResponse_CellChunk{
		{
			RowKey:          []byte("row1"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte(""),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
		{
			RowKey:          []byte("row3"),
			FamilyName:      &wrappers.StringValue{Value: "cf"},
			Qualifier:       &wrappers.BytesValue{Value: []byte("cq")},
			TimestampMicros: 1000,
			Value:           []byte("a"),
			RowStatus: &btpb.ReadRowsResponse_CellChunk_CommitRow{
				CommitRow: true,
			},
		},
	}
	if diff := cmp.Diff(gotChunks, wantChunks, cmp.Comparer(proto.Equal)); diff != "" {
		t.Fatalf("Response chunks mismatch: got: + want -\n%s", diff)
	}
}

func readRows(ctx context.Context, s *clientIntf, req *btpb.ReadRowsRequest) ([]*btpb.ReadRowsResponse, error) {
	stream, err := s.ReadRows(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	var ret []*btpb.ReadRowsResponse
	var lastRow, lastQual []byte
	var lastFamily string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		// Resolve sparse cell chunks.
		for _, c := range msg.Chunks {
			if c.RowKey == nil {
				c.RowKey = lastRow
			} else {
				lastRow = c.RowKey
			}
			if c.FamilyName == nil {
				c.FamilyName = &wrappers.StringValue{Value: lastFamily}
			} else {
				lastFamily = c.FamilyName.Value
			}
			if c.Qualifier == nil {
				c.Qualifier = &wrappers.BytesValue{Value: lastQual}
			} else {
				lastQual = c.Qualifier.Value
			}
		}
		ret = append(ret, msg)
	}
}

func sampleRowKeys(ctx context.Context, s *clientIntf, req *btpb.SampleRowKeysRequest) ([]*btpb.SampleRowKeysResponse, error) {
	stream, err := s.SampleRowKeys(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	var ret []*btpb.SampleRowKeysResponse
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return ret, nil
		} else if err != nil {
			return ret, err
		}
		ret = append(ret, msg)
	}
}
