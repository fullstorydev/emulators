package bttest

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var (
	testMeta = []struct {
		name string
		f    func(*testing.T)
	}{
		{"TestConcurrentMutationsReadModify", TestConcurrentMutationsReadModify},
		{"TestCreateTableResponse", TestCreateTableResponse},
		{"TestCreateTableWithFamily", TestCreateTableWithFamily},
		{"TestSampleRowKeys", TestSampleRowKeys},
		{"TestTableRowsConcurrent", TestTableRowsConcurrent},
		{"TestModifyColumnFamilies", TestModifyColumnFamilies},
		{"TestDropRowRange", TestDropRowRange},
		{"TestCheckTimestampMaxValue", TestCheckTimestampMaxValue},
		{"TestReadRows", TestReadRows},
		{"TestReadRowsError", TestReadRowsError},
		{"TestReadRowsAfterDeletion", TestReadRowsAfterDeletion},
		{"TestReadRowsOrder", TestReadRowsOrder},
		{"TestReadRowsWithlabelTransformer", TestReadRowsWithlabelTransformer},
		{"TestCheckAndMutateRowWithoutPredicate", TestCheckAndMutateRowWithoutPredicate},
		{"TestCheckAndMutateRowWithPredicate", TestCheckAndMutateRowWithPredicate},
		{"TestServer_ReadModifyWriteRow", TestServer_ReadModifyWriteRow},
		{"TestFilters", TestFilters},
		{"Test_Mutation_DeleteFromColumn", Test_Mutation_DeleteFromColumn},
		{"TestFilterRowWithSingleColumnQualifier", TestFilterRowWithSingleColumnQualifier},
		{"TestValueFilterRowWithAlternationInRegex", TestValueFilterRowWithAlternationInRegex},
	}
)

func TestLevelDbMem(t *testing.T) {
	clientIntfFuncs[t.Name()] = func(t *testing.T, name string) (context.Context, *clientIntf, bool) {
		ctx := context.Background()

		svr := &server{
			tables:  make(map[string]*table),
			storage: LeveldbMemStorage{},
			clock: func() time.Time {
				return time.Unix(0, 0).UTC()
			},
		}

		cl := &clientIntf{
			parent:                   fmt.Sprintf("projects/%s/instances/%s", "project", "cluster"),
			name:                     name,
			tblName:                  fmt.Sprintf("projects/%s/instances/%s/tables/%s", "project", "cluster", name),
			BigtableClient:           btServer2Client{s: svr},
			BigtableTableAdminClient: btServer2AdminClient{s: svr},
		}

		return ctx, cl, false
	}
	for _, tc := range testMeta {
		t.Run(tc.name, tc.f)
	}
}

func TestLevelDbDisk(t *testing.T) {
	clientIntfFuncs[t.Name()] = func(t *testing.T, name string) (context.Context, *clientIntf, bool) {
		ctx := context.Background()

		svr := &server{
			tables:  make(map[string]*table),
			storage: LeveldbDiskStorage{Root: "./test-out"},
			clock: func() time.Time {
				return time.Unix(0, 0).UTC()
			},
		}

		cl := &clientIntf{
			parent:                   fmt.Sprintf("projects/%s/instances/%s", "project", "cluster"),
			name:                     name,
			tblName:                  fmt.Sprintf("projects/%s/instances/%s/tables/%s", "project", "cluster", name),
			BigtableClient:           btServer2Client{s: svr},
			BigtableTableAdminClient: btServer2AdminClient{s: svr},
		}

		return ctx, cl, false
	}
	for _, tc := range testMeta {
		t.Run(tc.name, tc.f)
	}
}
