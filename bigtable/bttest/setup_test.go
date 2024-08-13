package bttest

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigtable"
	btapb "cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type clientIntfFunc func(t *testing.T, name string) (context.Context, *clientIntf, bool)

var (
	clientIntfFuncs = map[string]clientIntfFunc{}
)

func newClient(t *testing.T) (context.Context, *clientIntf, bool) {
	t.Parallel()
	ctx := context.Background()

	parts := strings.SplitN(t.Name(), "/", 2)
	if len(parts) == 2 {
		return clientIntfFuncs[parts[0]](t, parts[1])
	}

	svr := &server{
		tables:  make(map[string]*table),
		storage: BtreeStorage{},
		clock: func() bigtable.Timestamp {
			return 0
		},
	}

	cl := &clientIntf{
		parent:                   fmt.Sprintf("projects/%s/instances/%s", "project", "cluster"),
		name:                     t.Name(),
		tblName:                  fmt.Sprintf("projects/%s/instances/%s/tables/%s", "project", "cluster", t.Name()),
		BigtableClient:           btServer2Client{s: svr},
		BigtableTableAdminClient: btServer2AdminClient{s: svr},
	}

	return ctx, cl, false
}

type streamAdapter struct {
	ctx  context.Context
	msgs []proto.Message
}

func (a *streamAdapter) SetHeader(md metadata.MD) error {
	return nil
}

func (a *streamAdapter) SendHeader(md metadata.MD) error {
	return nil
}

func (a *streamAdapter) SetTrailer(md metadata.MD) {
}

func (a *streamAdapter) Header() (metadata.MD, error) {
	return nil, nil
}

func (a *streamAdapter) Trailer() metadata.MD {
	return nil
}

func (a *streamAdapter) CloseSend() error {
	return nil
}

func (a *streamAdapter) Context() context.Context {
	return a.ctx
}

func (a *streamAdapter) SendMsg(m interface{}) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	a.msgs = append(a.msgs, m.(proto.Message))
	return nil
}

func (a *streamAdapter) RecvMsg(m interface{}) error {
	if len(a.msgs) == 0 {
		return io.EOF
	}
	if err := a.ctx.Err(); err != nil {
		return err
	}
	ret := a.msgs[0]
	reflect.ValueOf(m).Elem().Set(reflect.ValueOf(ret).Elem())
	a.msgs = a.msgs[1:]
	return nil
}

var _ grpc.ClientStream = (*streamAdapter)(nil)
var _ grpc.ServerStream = (*streamAdapter)(nil)

type btServer2Client struct {
	s btpb.BigtableServer
	btpb.BigtableClient
}

type rrAdapter struct {
	streamAdapter
}

func (r *rrAdapter) Send(response *btpb.ReadRowsResponse) error {
	return r.streamAdapter.SendMsg(response)

}
func (r *rrAdapter) Recv() (*btpb.ReadRowsResponse, error) {
	ret := &btpb.ReadRowsResponse{}
	return ret, r.streamAdapter.RecvMsg(ret)
}

func (b btServer2Client) ReadRows(ctx context.Context, in *btpb.ReadRowsRequest, _ ...grpc.CallOption) (btpb.Bigtable_ReadRowsClient, error) {
	cl := &rrAdapter{streamAdapter{ctx: ctx}}
	err := b.s.ReadRows(in, cl)
	return cl, err
}

type srkAdapter struct {
	streamAdapter
}

func (r *srkAdapter) Send(response *btpb.SampleRowKeysResponse) error {
	return r.streamAdapter.SendMsg(response)

}
func (r *srkAdapter) Recv() (*btpb.SampleRowKeysResponse, error) {
	ret := &btpb.SampleRowKeysResponse{}
	return ret, r.streamAdapter.RecvMsg(ret)
}

func (b btServer2Client) SampleRowKeys(ctx context.Context, in *btpb.SampleRowKeysRequest, _ ...grpc.CallOption) (btpb.Bigtable_SampleRowKeysClient, error) {
	cl := &srkAdapter{streamAdapter{ctx: ctx}}
	err := b.s.SampleRowKeys(in, cl)
	return cl, err
}

func (b btServer2Client) MutateRow(ctx context.Context, in *btpb.MutateRowRequest, _ ...grpc.CallOption) (*btpb.MutateRowResponse, error) {
	return b.s.MutateRow(ctx, in)
}

type mrAdapter struct {
	streamAdapter
}

func (r *mrAdapter) Send(response *btpb.MutateRowsResponse) error {
	return r.streamAdapter.SendMsg(response)

}
func (r *mrAdapter) Recv() (*btpb.MutateRowsResponse, error) {
	ret := &btpb.MutateRowsResponse{}
	return ret, r.streamAdapter.RecvMsg(ret)
}

func (b btServer2Client) MutateRows(ctx context.Context, in *btpb.MutateRowsRequest, _ ...grpc.CallOption) (btpb.Bigtable_MutateRowsClient, error) {
	cl := &mrAdapter{streamAdapter{ctx: ctx}}
	err := b.s.MutateRows(in, cl)
	return cl, err
}

func (b btServer2Client) CheckAndMutateRow(ctx context.Context, in *btpb.CheckAndMutateRowRequest, _ ...grpc.CallOption) (*btpb.CheckAndMutateRowResponse, error) {
	return b.s.CheckAndMutateRow(ctx, in)
}

func (b btServer2Client) ReadModifyWriteRow(ctx context.Context, in *btpb.ReadModifyWriteRowRequest, _ ...grpc.CallOption) (*btpb.ReadModifyWriteRowResponse, error) {
	return b.s.ReadModifyWriteRow(ctx, in)
}

type btServer2AdminClient struct {
	s btapb.BigtableTableAdminServer
	btapb.BigtableTableAdminClient
}

func (b btServer2AdminClient) CreateTable(ctx context.Context, in *btapb.CreateTableRequest, _ ...grpc.CallOption) (*btapb.Table, error) {
	return b.s.CreateTable(ctx, in)
}

func (b btServer2AdminClient) ListTables(ctx context.Context, in *btapb.ListTablesRequest, _ ...grpc.CallOption) (*btapb.ListTablesResponse, error) {
	return b.s.ListTables(ctx, in)
}

func (b btServer2AdminClient) GetTable(ctx context.Context, in *btapb.GetTableRequest, _ ...grpc.CallOption) (*btapb.Table, error) {
	return b.s.GetTable(ctx, in)
}

func (b btServer2AdminClient) DeleteTable(ctx context.Context, in *btapb.DeleteTableRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return b.s.DeleteTable(ctx, in)
}

func (b btServer2AdminClient) ModifyColumnFamilies(ctx context.Context, in *btapb.ModifyColumnFamiliesRequest, _ ...grpc.CallOption) (*btapb.Table, error) {
	return b.s.ModifyColumnFamilies(ctx, in)
}

func (b btServer2AdminClient) DropRowRange(ctx context.Context, in *btapb.DropRowRangeRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return b.s.DropRowRange(ctx, in)
}

func (b btServer2AdminClient) GenerateConsistencyToken(ctx context.Context, in *btapb.GenerateConsistencyTokenRequest, _ ...grpc.CallOption) (*btapb.GenerateConsistencyTokenResponse, error) {
	return b.s.GenerateConsistencyToken(ctx, in)
}

func (b btServer2AdminClient) CheckConsistency(ctx context.Context, in *btapb.CheckConsistencyRequest, _ ...grpc.CallOption) (*btapb.CheckConsistencyResponse, error) {
	return b.s.CheckConsistency(ctx, in)
}
