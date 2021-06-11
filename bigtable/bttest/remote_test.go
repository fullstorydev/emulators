package bttest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/bigtableadmin/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// e.g. PROJECT_ID=fs-playpen INSTANCE_ID=playpen-test1 go test -v fs/gcloud/bt/bttest -run TestRemote

var (
	testMeta = []struct {
		name string
		f    func(*testing.T)
	}{
		// {"TestConcurrentMutationsReadModify", TestConcurrentMutationsReadModify},
		{"TestCreateTableResponse", TestCreateTableResponse},
		{"TestCreateTableWithFamily", TestCreateTableWithFamily},
		// {"TestSampleRowKeys", TestSampleRowKeys}, cannot make strong guarantees on real bigtable
		// {"TestTableRowsConcurrent", TestTableRowsConcurrent},
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

func TestRemote(t *testing.T) {
	project := os.Getenv("PROJECT_ID")
	instance := os.Getenv("INSTANCE_ID")
	if project == "" || instance == "" {
		t.Skip("PROJECT_ID and INSTANCE_ID must be set to run this")
	}

	ctx := context.Background()
	btcPool, err := newClientPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	btcaPool, err := newAdminPool(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clientIntfFuncs[t.Name()] = func(t *testing.T) (context.Context, *clientIntf, bool) {
		return newRemoteServer(t, btcPool, btcaPool, project, instance)
	}
	for _, tc := range testMeta {
		t.Run(tc.name, tc.f)
	}
}

func newClientPool(ctx context.Context) (grpc.ClientConnInterface, error) {
	o := []option.ClientOption{
		internaloption.WithDefaultEndpoint("bigtable.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("bigtable.mtls.googleapis.com:443"),
		option.WithScopes(bigtable.Scope),
		option.WithUserAgent("cbt-go/v1.6.0"),
		option.WithGRPCConnectionPool(4),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1<<28), grpc.MaxCallRecvMsgSize(1<<28))),
		option.WithGRPCDialOption(grpc.WithBlock()),
	}
	// Attempts direct access to spanner service over gRPC to improve throughput,
	// whether the attempt is allowed is totally controlled by service owner.
	// o = append(o, internaloption.EnableDirectPath(true))
	return gtransport.DialPool(ctx, o...)
}

func newAdminPool(ctx context.Context) (grpc.ClientConnInterface, error) {
	o := []option.ClientOption{
		internaloption.WithDefaultEndpoint("bigtableadmin.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("bigtableadmin.mtls.googleapis.com:443"),
		option.WithScopes(bigtable.AdminScope),
		option.WithUserAgent("cbt-go/v1.6.0"),
		option.WithGRPCConnectionPool(4),
		option.WithGRPCDialOption(grpc.WithBlock()),
	}
	o = append(o, option.WithScopes(bigtableadmin.CloudPlatformScope))
	return gtransport.DialPool(ctx, o...)
}

func newRemoteServer(t *testing.T, btcPool, btcaPool grpc.ClientConnInterface, project, instance string) (context.Context, *clientIntf, bool) {
	nameParts := strings.Split(t.Name(), "/")
	parent := fmt.Sprintf("projects/%s/instances/%s", project, instance)
	tbl := fmt.Sprintf("projects/%s/instances/%s/tables/%s", project, instance, nameParts[len(nameParts)-1])
	ret := &clientIntf{
		parent:                   parent,
		name:                     nameParts[1],
		tblName:                  tbl,
		BigtableClient:           btpb.NewBigtableClient(btcPool),
		BigtableTableAdminClient: btapb.NewBigtableTableAdminClient(btcaPool),
	}

	md := metadata.New(map[string]string{"google-cloud-resource-prefix": ret.parent})
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err := ret.DropRowRange(ctx, &btapb.DropRowRangeRequest{
		Name:   ret.tblName,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{DeleteAllDataFromTable: true},
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return ctx, ret, false
		} else {
			t.Fatal(err)
		}
	}

	return ctx, ret, true
}
