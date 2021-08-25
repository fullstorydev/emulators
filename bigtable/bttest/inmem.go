/*
Copyright 2015 Google LLC

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

/*
Package bttest contains test helpers for working with the bigtable package.

To use a Server, create it, and then connect to it with no security:
(The project/instance values are ignored.)
	srv, err := bttest.NewServer("localhost:0")
	...
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	...
	client, err := bigtable.NewClient(ctx, proj, instance,
	        option.WithGRPCConn(conn))
	...
*/
package bttest // import "github.com/fullstorydev/emulators/bigtable/bttest"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	emptypb "github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	statpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"rsc.io/binaryregexp"
)

const (
	// MilliSeconds field of the minimum valid Timestamp.
	minValidMilliSeconds = 0

	// MilliSeconds field of the max valid Timestamp.
	// Must match the max value of type TimestampMicros (int64)
	// truncated to the millis granularity by subtracting a remainder of 1000.
	maxValidMilliSeconds = math.MaxInt64 - math.MaxInt64%1000
)

var validLabelTransformer = regexp.MustCompile(`[a-z0-9\-]{1,15}`)

// Server is an in-memory Cloud Bigtable fake.
// It is unauthenticated, and only a rough approximation.
type Server struct {
	Addr string

	l   net.Listener
	srv *grpc.Server
	s   *server
}

// server is the real implementation of the fake.
// It is a separate and unexported type so the API won't be cluttered with
// methods that are only relevant to the fake's implementation.
type server struct {
	storage Storage
	clock   func() time.Time

	mu     sync.Mutex
	tables map[string]*table // keyed by fully qualified name
	done   chan struct{}     // closed when server shuts down

	// Any unimplemented methods will return unimplemented.
	*btapb.UnimplementedBigtableTableAdminServer
	*btapb.UnimplementedBigtableInstanceAdminServer
	*btpb.UnimplementedBigtableServer
}

// NewServer creates a new Server.
// The Server will be listening for gRPC connections, without TLS,
// on the provided address. The resolved address is named by the Addr field.
func NewServer(laddr string, opt ...grpc.ServerOption) (*Server, error) {
	return NewServerWithOptions(laddr, Options{
		GrpcOpts: opt,
	})
}

type Options struct {
	// A storage layer to use; if nil, defaults to LeveldbMemStorage.
	Storage Storage
	// The clock to use use; if nil, defaults to time.Now().
	Clock func() time.Time

	// Grpc server options.
	GrpcOpts []grpc.ServerOption
}

// NewServerWithOptions creates a new Server with the given options.
// The Server will be listening for gRPC connections, without TLS,
// on the provided address. The resolved address is named by the Addr field.
func NewServerWithOptions(laddr string, opt Options) (*Server, error) {
	if opt.Storage == nil {
		opt.Storage = LeveldbMemStorage{}
	}
	if opt.Clock == nil {
		opt.Clock = time.Now
	}
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, err
	}

	s := &Server{
		Addr: l.Addr().String(),
		l:    l,
		srv:  grpc.NewServer(opt.GrpcOpts...),
		s: &server{
			storage: opt.Storage,
			tables:  make(map[string]*table),
			clock:   opt.Clock,
			done:    make(chan struct{}),
		},
	}

	// Init from storage.
	for _, tbl := range s.s.storage.GetTables() {
		rows := s.s.storage.Open(tbl)
		s.s.tables[tbl.Name] = newTable(tbl, rows)
	}

	btapb.RegisterBigtableInstanceAdminServer(s.srv, s.s)
	btapb.RegisterBigtableTableAdminServer(s.srv, s.s)
	btpb.RegisterBigtableServer(s.srv, s.s)

	go func() {
		_ = s.srv.Serve(s.l)
	}()
	go s.s.gcloop()

	return s, nil
}

// Close shuts down the server.
func (s *Server) Close() {
	close(s.s.done)
	s.srv.Stop()
	_ = s.l.Close()

	var tbls []*table
	s.s.mu.Lock()
	for _, t := range s.s.tables {
		tbls = append(tbls, t)
	}
	s.s.mu.Unlock()

	for _, tbl := range tbls {
		func() {
			tbl.mu.Lock()
			defer tbl.mu.Unlock()
			tbl.rows.Close()
		}()
	}
}

func (s *server) CreateTable(ctx context.Context, req *btapb.CreateTableRequest) (*btapb.Table, error) {
	tbl := req.Parent + "/tables/" + req.TableId

	s.mu.Lock()
	if _, ok := s.tables[tbl]; ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "table %q already exists", tbl)
	}
	if req.Table == nil {
		req.Table = &btapb.Table{}
	}
	req.Table.Name = tbl
	rows := s.storage.Create(req.Table)
	s.tables[tbl] = newTable(req.Table, rows)

	s.mu.Unlock()

	ct := &btapb.Table{
		Name:           tbl,
		ColumnFamilies: req.GetTable().GetColumnFamilies(),
		Granularity:    req.GetTable().GetGranularity(),
	}
	if ct.Granularity == 0 {
		ct.Granularity = btapb.Table_MILLIS
	}
	return ct, nil
}

func (s *server) ListTables(ctx context.Context, req *btapb.ListTablesRequest) (*btapb.ListTablesResponse, error) {
	res := &btapb.ListTablesResponse{}
	prefix := req.Parent + "/tables/"

	s.mu.Lock()
	for tbl := range s.tables {
		if strings.HasPrefix(tbl, prefix) {
			res.Tables = append(res.Tables, &btapb.Table{Name: tbl})
		}
	}
	s.mu.Unlock()

	return res, nil
}

func (s *server) GetTable(ctx context.Context, req *btapb.GetTableRequest) (*btapb.Table, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.Name]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return tbl.def, nil
}

func (s *server) DeleteTable(ctx context.Context, req *btapb.DeleteTableRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[req.Name]; !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}
	delete(s.tables, req.Name)
	return &emptypb.Empty{}, nil
}

func (s *server) ModifyColumnFamilies(ctx context.Context, req *btapb.ModifyColumnFamiliesRequest) (*btapb.Table, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.Name]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	cfs := tbl.def.ColumnFamilies

	for _, mod := range req.Modifications {
		if create := mod.GetCreate(); create != nil {
			if _, ok := cfs[mod.Id]; ok {
				return nil, status.Errorf(codes.AlreadyExists, "family %q already exists", mod.Id)
			}
			cfs[mod.Id] = &btapb.ColumnFamily{
				GcRule: create.GcRule,
			}
		} else if mod.GetDrop() {
			if _, ok := cfs[mod.Id]; !ok {
				return nil, fmt.Errorf("can't delete unknown family %q", mod.Id)
			}
			delete(cfs, mod.Id)

			// Purge all data for this column family
			tbl.rows.Ascend(func(r *btpb.Row) bool {
				r, changed := scrubRow(r, tbl.cols())
				if changed {
					tbl.rows.ReplaceOrInsert(r)
				}
				return true
			})
		} else if modify := mod.GetUpdate(); modify != nil {
			cf, ok := cfs[mod.Id]
			if !ok {
				return nil, fmt.Errorf("no such family %q", mod.Id)
			}
			// assume that we ALWAYS want to replace by the new setting
			// we may need partial update through
			cf.GcRule = modify.GcRule
		}
	}

	s.storage.SetTableMeta(tbl.def)
	return tbl.def, nil
}

func (s *server) DropRowRange(ctx context.Context, req *btapb.DropRowRangeRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.Name]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if req.GetDeleteAllDataFromTable() {
		tbl.rows.Clear()
	} else {
		// Delete rows by prefix.
		prefixBytes := req.GetRowKeyPrefix()
		if prefixBytes == nil {
			return nil, fmt.Errorf("missing row key prefix")
		}

		// Rows does not specify what happens if rows are deleted during
		// iteration, and it provides no "delete range" method.
		// So we collect the rows first, then delete them one by one.
		var rowsToDelete []keyType
		tbl.rows.AscendGreaterOrEqual(prefixBytes, func(r *btpb.Row) bool {
			if bytes.HasPrefix(r.Key, prefixBytes) {
				rowsToDelete = append(rowsToDelete, r.Key)
				return true
			}
			return false // stop iteration
		})
		for _, r := range rowsToDelete {
			tbl.rows.Delete(r)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *server) GenerateConsistencyToken(ctx context.Context, req *btapb.GenerateConsistencyTokenRequest) (*btapb.GenerateConsistencyTokenResponse, error) {
	// Check that the table exists.
	_, ok := s.tables[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	return &btapb.GenerateConsistencyTokenResponse{
		ConsistencyToken: "TokenFor-" + req.Name,
	}, nil
}

func (s *server) CheckConsistency(ctx context.Context, req *btapb.CheckConsistencyRequest) (*btapb.CheckConsistencyResponse, error) {
	// Check that the table exists.
	_, ok := s.tables[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.Name)
	}

	// Check this is the right token.
	if req.ConsistencyToken != "TokenFor-"+req.Name {
		return nil, status.Errorf(codes.InvalidArgument, "token %q not valid", req.ConsistencyToken)
	}

	// Single cluster instances are always consistent.
	return &btapb.CheckConsistencyResponse{
		Consistent: true,
	}, nil
}

type simpleRange struct {
	start, end keyType
}

// Returns a sorted, normalized list of ranges to traverse.
func mergeRowRanges(explicit []keyType, rrs []*btpb.RowRange) []simpleRange {
	var srs []simpleRange
	for _, k := range explicit {
		srs = append(srs, simpleRange{
			start: k,
			end:   append(k, 0),
		})
	}
	for _, rr := range rrs {
		var sr simpleRange
		switch sk := rr.StartKey.(type) {
		case *btpb.RowRange_StartKeyClosed:
			sr.start = sk.StartKeyClosed
		case *btpb.RowRange_StartKeyOpen:
			sr.start = append(sk.StartKeyOpen, 0)
		}
		switch ek := rr.EndKey.(type) {
		case *btpb.RowRange_EndKeyClosed:
			sr.end = append(ek.EndKeyClosed, 0)
		case *btpb.RowRange_EndKeyOpen:
			sr.end = ek.EndKeyOpen
		}
		srs = append(srs, sr)
	}
	return mergeSimpleRanges(srs)
}

func mergeSimpleRanges(srs []simpleRange) []simpleRange {
	if len(srs) == 0 {
		return srs
	}

	// Special case end compare: the empty key is greater than a non-empty key
	endCmp := func(a, b simpleRange) int {
		switch {
		case len(a.end) == 0 && len(b.end) == 0:
			return 0 // both empty
		case len(b.end) == 0:
			return -1 // b is infinite, therefore a < b
		case len(a.end) == 0:
			return 1 // a is infinite, therefore a > b
		default:
			return bytes.Compare(a.end, b.end)
		}
	}

	sort.Slice(srs, func(i, j int) bool {
		if cmp := bytes.Compare(srs[i].start, srs[j].start); cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
		if cmp := endCmp(srs[i], srs[j]); cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
		return false
	})

	merge := func(a simpleRange, b simpleRange) (simpleRange, bool) {
		// a and b are disjoint if a's range is not infinite, and a's end less than b's start.
		if len(a.end) > 0 && bytes.Compare(a.end, b.start) < 0 {
			return simpleRange{}, false
		}

		// a and b are not disjoint, so we can merge them
		var end keyType
		if endCmp(a, b) < 0 {
			end = b.end
		} else {
			end = a.end
		}
		return simpleRange{
			start: a.start,
			end:   end,
		}, true
	}

	// Merge.
	last := 0
	for i := range srs {
		if i == 0 {
			continue
		}
		merged, didMerge := merge(srs[last], srs[i])
		if didMerge {
			srs[last] = merged
		} else {
			last++
			srs[last] = srs[i]
		}
	}
	return srs[:last+1]
}

func (s *server) ReadRows(req *btpb.ReadRowsRequest, stream btpb.Bigtable_ReadRowsServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	if err := validateRowRanges(req); err != nil {
		return err
	}

	srs := []simpleRange{{}} // infinite range unless specified
	if len(req.GetRows().GetRowKeys())+len(req.GetRows().GetRowRanges()) > 0 {
		srs = mergeRowRanges(req.GetRows().GetRowKeys(), req.GetRows().GetRowRanges())
	}

	defer tbl.read()
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	limit := int(req.RowsLimit)
	count := 0

	var err error
	var cb chunkBuilder
	sendResponse := func() error {
		// Reverse the lock while streaming the row out.
		tbl.mu.RUnlock()
		defer tbl.mu.RLock()
		return stream.Send(&btpb.ReadRowsResponse{Chunks: cb.chunks})
	}

	for _, sr := range srs {
		addRow := func(r *btpb.Row) bool {
			if limit > 0 && count >= limit {
				return false
			}

			if len(r.Families) == 0 {
				return true
			}

			var match bool
			match, err = filterRow(req.Filter, r)
			if err != nil {
				return false
			} else if !match {
				return true
			}

			if added := cb.add(tbl.cols(), r); added {
				count++
			}

			if len(cb.chunks) > 1024 {
				err = sendResponse()
				if err != nil {
					return false
				}
				cb.reset()
			}
			return true
		}

		switch {
		case len(sr.start) == 0 && len(sr.end) == 0:
			tbl.rows.Ascend(addRow) // all rows
		case len(sr.start) == 0:
			tbl.rows.AscendLessThan(sr.end, addRow)
		case len(sr.end) == 0:
			tbl.rows.AscendGreaterOrEqual(sr.start, addRow)
		default:
			tbl.rows.AscendRange(sr.start, sr.end, addRow)
		}

		if err != nil {
			return err
		}
	}
	if err == nil && len(cb.chunks) > 0 {
		err = sendResponse()
	}
	return err
}

type chunkBuilder struct {
	chunks []*btpb.ReadRowsResponse_CellChunk
}

func (cb *chunkBuilder) reset() {
	cb.chunks = nil
}

func (cb *chunkBuilder) add(cols map[string]*btapb.ColumnFamily, r *btpb.Row) bool {
	scrubRow(r, cols)
	newRow := true
	for _, fam := range r.Families {
		newFam := true
		for _, col := range fam.Columns {
			newCol := true
			cells := col.Cells
			if len(cells) == 0 {
				continue
			}
			for _, cell := range cells {
				chunk := &btpb.ReadRowsResponse_CellChunk{
					TimestampMicros: cell.TimestampMicros,
					Value:           cell.Value,
					Labels:          cell.Labels,
				}
				if newRow {
					chunk.RowKey = r.Key
					newRow = false
				}
				if newFam {
					chunk.FamilyName = &wrappers.StringValue{Value: fam.Name}
					newFam = false
				}
				if newCol {
					chunk.Qualifier = &wrappers.BytesValue{Value: col.Qualifier}
					newCol = false
				}

				// TODO(scottb): if Value is massive, we might have to break it up into multiple responses.
				cb.chunks = append(cb.chunks, chunk)
			}
		}
	}
	// We can't have a cell with just COMMIT set, which would imply a new empty cell.
	// So modify the last cell to have the COMMIT flag set.
	if len(cb.chunks) > 0 {
		cb.chunks[len(cb.chunks)-1].RowStatus = &btpb.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true}
	}
	return true
}

// filterRow modifies a row with the given filter. Returns true if at least one cell from the row matches,
// false otherwise. If a filter is invalid, filterRow returns false and an error.
func filterRow(f *btpb.RowFilter, r *btpb.Row) (bool, error) {
	if f == nil {
		return true, nil
	}
	// Handle filters that apply beyond just including/excluding cells.
	switch f := f.Filter.(type) {
	case *btpb.RowFilter_BlockAllFilter:
		if !f.BlockAllFilter {
			return false, status.Errorf(codes.InvalidArgument, "block_all_filter must be true if set")
		}
		return false, nil
	case *btpb.RowFilter_PassAllFilter:
		if !f.PassAllFilter {
			return false, status.Errorf(codes.InvalidArgument, "pass_all_filter must be true if set")
		}
		return true, nil
	case *btpb.RowFilter_Chain_:
		if len(f.Chain.Filters) < 2 {
			return false, status.Errorf(codes.InvalidArgument, "Chain must contain at least two RowFilters")
		}
		for _, sub := range f.Chain.Filters {
			match, err := filterRow(sub, r)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	case *btpb.RowFilter_Interleave_:
		if len(f.Interleave.Filters) < 2 {
			return false, status.Errorf(codes.InvalidArgument, "Interleave must contain at least two RowFilters")
		}
		srs := make([]*btpb.Row, 0, len(f.Interleave.Filters))
		for _, sub := range f.Interleave.Filters {
			sr := copyRow(r)
			match, err := filterRow(sub, sr)
			if err != nil {
				return false, err
			}
			if match {
				srs = append(srs, sr)
			}
		}
		// merge
		// TODO(dsymonds): is this correct?
		r.Families = nil
		for _, sr := range srs {
			for _, fam := range sr.Families {
				f := getOrCreateFamily(r, fam.Name)
				for _, col := range fam.Columns {
					c := getOrCreateColumn(f, col.Qualifier)
					c.Cells = append(c.Cells, col.Cells...)
				}
			}
		}
		var count int
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				sort.Sort(byDescTS(col.Cells))
				count += len(col.Cells)
			}
		}
		return count > 0, nil
	case *btpb.RowFilter_CellsPerColumnLimitFilter:
		lim := int(f.CellsPerColumnLimitFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > lim {
					col.Cells = col.Cells[:lim]
				}
			}
		}
		return true, nil
	case *btpb.RowFilter_Condition_:
		match, err := filterRow(f.Condition.PredicateFilter, copyRow(r))
		if err != nil {
			return false, err
		}
		if match {
			if f.Condition.TrueFilter == nil {
				return false, nil
			}
			return filterRow(f.Condition.TrueFilter, r)
		}
		if f.Condition.FalseFilter == nil {
			return false, nil
		}
		return filterRow(f.Condition.FalseFilter, r)
	case *btpb.RowFilter_RowKeyRegexFilter:
		rx, err := newRegexp(f.RowKeyRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'rowkey_regex_filter' : %v", err)
		}
		if !rx.Match(r.Key) {
			return false, nil
		}
	case *btpb.RowFilter_CellsPerRowLimitFilter:
		// Grab the first n cells in the row.
		lim := int(f.CellsPerRowLimitFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > lim {
					col.Cells = col.Cells[:lim]
					lim = 0
				} else {
					lim -= len(col.Cells)
				}
			}
		}
		return true, nil
	case *btpb.RowFilter_CellsPerRowOffsetFilter:
		// Skip the first n cells in the row.
		offset := int(f.CellsPerRowOffsetFilter)
		for _, fam := range r.Families {
			for _, col := range fam.Columns {
				if len(col.Cells) > offset {
					col.Cells = col.Cells[offset:]
					return true, nil
				}
				col.Cells = col.Cells[:0]
				offset -= len(col.Cells)
			}
		}
		return true, nil
	case *btpb.RowFilter_RowSampleFilter:
		// The row sample filter "matches all cells from a row with probability
		// p, and matches no cells from the row with probability 1-p."
		// See https://github.com/googleapis/googleapis/blob/master/google/bigtable/v2/data.proto
		if f.RowSampleFilter <= 0.0 || f.RowSampleFilter >= 1.0 {
			return false, status.Error(codes.InvalidArgument, "row_sample_filter argument must be between 0.0 and 1.0")
		}
		return randFloat() < f.RowSampleFilter, nil
	}

	// Any other case, operate on a per-cell basis.
	cellCount := 0
	for _, fam := range r.Families {
		for _, col := range fam.Columns {
			filtered, err := filterCells(f, fam.Name, col.Qualifier, col.Cells)
			if err != nil {
				return false, err
			}
			col.Cells = filtered
			cellCount += len(col.Cells)
		}
	}
	return cellCount > 0, nil
}

var randFloat = rand.Float64

func filterCells(f *btpb.RowFilter, fam string, col []byte, cs []*btpb.Cell) ([]*btpb.Cell, error) {
	var ret []*btpb.Cell
	for _, cell := range cs {
		include, err := includeCell(f, fam, col, cell)
		if err != nil {
			return nil, err
		}
		if include {
			cell, err = modifyCell(f, cell)
			if err != nil {
				return nil, err
			}
			ret = append(ret, cell)
		}
	}
	return ret, nil
}

func modifyCell(f *btpb.RowFilter, c *btpb.Cell) (*btpb.Cell, error) {
	if f == nil {
		return c, nil
	}
	// Consider filters that may modify the cell contents
	switch filter := f.Filter.(type) {
	case *btpb.RowFilter_StripValueTransformer:
		return &btpb.Cell{TimestampMicros: c.TimestampMicros}, nil
	case *btpb.RowFilter_ApplyLabelTransformer:
		if !validLabelTransformer.MatchString(filter.ApplyLabelTransformer) {
			return &btpb.Cell{}, status.Errorf(
				codes.InvalidArgument,
				`apply_label_transformer must match RE2([a-z0-9\-]+), but found %v`,
				filter.ApplyLabelTransformer,
			)
		}
		return &btpb.Cell{
			TimestampMicros: c.TimestampMicros,
			Value:           c.Value,
			Labels:          []string{filter.ApplyLabelTransformer},
		}, nil
	default:
		return c, nil
	}
}

func includeCell(f *btpb.RowFilter, fam string, col []byte, cell *btpb.Cell) (bool, error) {
	if f == nil {
		return true, nil
	}
	// TODO(dsymonds): Implement many more filters.
	switch f := f.Filter.(type) {
	case *btpb.RowFilter_CellsPerColumnLimitFilter:
		// Don't log, row-level filter
		return true, nil
	case *btpb.RowFilter_RowKeyRegexFilter:
		// Don't log, row-level filter
		return true, nil
	case *btpb.RowFilter_StripValueTransformer:
		// Don't log, cell-modifying filter
		return true, nil
	case *btpb.RowFilter_ApplyLabelTransformer:
		// Don't log, cell-modifying filter
		return true, nil
	default:
		log.Printf("WARNING: don't know how to handle filter of type %T (ignoring it)", f)
		return true, nil
	case *btpb.RowFilter_FamilyNameRegexFilter:
		rx, err := newRegexp([]byte(f.FamilyNameRegexFilter))
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'family_name_regex_filter' : %v", err)
		}
		return rx.MatchString(fam), nil
	case *btpb.RowFilter_ColumnQualifierRegexFilter:
		rx, err := newRegexp(f.ColumnQualifierRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'column_qualifier_regex_filter' : %v", err)
		}
		return rx.Match(col), nil
	case *btpb.RowFilter_ValueRegexFilter:
		rx, err := newRegexp(f.ValueRegexFilter)
		if err != nil {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'value_regex_filter' : %v", err)
		}
		return rx.Match(cell.Value), nil
	case *btpb.RowFilter_ColumnRangeFilter:
		if fam != f.ColumnRangeFilter.FamilyName {
			return false, nil
		}
		// Start qualifier defaults to empty string closed
		inRangeStart := func() bool { return bytes.Compare(col, nil) >= 0 }
		switch sq := f.ColumnRangeFilter.StartQualifier.(type) {
		case *btpb.ColumnRange_StartQualifierOpen:
			inRangeStart = func() bool { return bytes.Compare(col, sq.StartQualifierOpen) > 0 }
		case *btpb.ColumnRange_StartQualifierClosed:
			inRangeStart = func() bool { return bytes.Compare(col, sq.StartQualifierClosed) >= 0 }
		}
		// End qualifier defaults to no upper boundary
		inRangeEnd := func() bool { return true }
		switch eq := f.ColumnRangeFilter.EndQualifier.(type) {
		case *btpb.ColumnRange_EndQualifierClosed:
			inRangeEnd = func() bool { return bytes.Compare(col, eq.EndQualifierClosed) <= 0 }
		case *btpb.ColumnRange_EndQualifierOpen:
			inRangeEnd = func() bool { return bytes.Compare(col, eq.EndQualifierOpen) < 0 }
		}
		return inRangeStart() && inRangeEnd(), nil
	case *btpb.RowFilter_TimestampRangeFilter:
		// Server should only support millisecond precision.
		if f.TimestampRangeFilter.StartTimestampMicros%int64(time.Millisecond/time.Microsecond) != 0 || f.TimestampRangeFilter.EndTimestampMicros%int64(time.Millisecond/time.Microsecond) != 0 {
			return false, status.Errorf(codes.InvalidArgument, "Error in field 'timestamp_range_filter'. Maximum precision allowed in filter is millisecond.\nGot:\nStart: %v\nEnd: %v", f.TimestampRangeFilter.StartTimestampMicros, f.TimestampRangeFilter.EndTimestampMicros)
		}
		// Lower bound is inclusive and defaults to 0, upper bound is exclusive and defaults to infinity.
		return cell.TimestampMicros >= f.TimestampRangeFilter.StartTimestampMicros &&
			(f.TimestampRangeFilter.EndTimestampMicros == 0 || cell.TimestampMicros < f.TimestampRangeFilter.EndTimestampMicros), nil
	case *btpb.RowFilter_ValueRangeFilter:
		v := cell.Value
		// Start value defaults to empty string closed
		inRangeStart := func() bool { return bytes.Compare(v, []byte{}) >= 0 }
		switch sv := f.ValueRangeFilter.StartValue.(type) {
		case *btpb.ValueRange_StartValueOpen:
			inRangeStart = func() bool { return bytes.Compare(v, sv.StartValueOpen) > 0 }
		case *btpb.ValueRange_StartValueClosed:
			inRangeStart = func() bool { return bytes.Compare(v, sv.StartValueClosed) >= 0 }
		}
		// End value defaults to no upper boundary
		inRangeEnd := func() bool { return true }
		switch ev := f.ValueRangeFilter.EndValue.(type) {
		case *btpb.ValueRange_EndValueClosed:
			inRangeEnd = func() bool { return bytes.Compare(v, ev.EndValueClosed) <= 0 }
		case *btpb.ValueRange_EndValueOpen:
			inRangeEnd = func() bool { return bytes.Compare(v, ev.EndValueOpen) < 0 }
		}
		return inRangeStart() && inRangeEnd(), nil
	}
}

// escapeUTF is used to escape non-ASCII characters in pattern strings passed
// to binaryregexp. This makes regexp column and row key matching work more
// closely to what's seen with the real BigTable.
func escapeUTF(in []byte) []byte {
	var toEsc int
	for _, c := range in {
		if c > 127 {
			toEsc++
		}
	}
	if toEsc == 0 {
		return in
	}
	// Each escaped byte becomes 4 bytes (byte a1 becomes \xA1)
	out := make([]byte, 0, len(in)+3*toEsc)
	for _, c := range in {
		if c > 127 {
			h, l := c>>4, c&0xF
			const conv = "0123456789ABCDEF"
			out = append(out, '\\', 'x', conv[h], conv[l])
		} else {
			out = append(out, c)
		}
	}
	return out
}

func newRegexp(pat []byte) (*binaryregexp.Regexp, error) {
	re, err := binaryregexp.Compile("^(?:" + string(escapeUTF(pat)) + ")$") // match entire target
	if err != nil {
		log.Printf("Bad pattern %q: %v", pat, err)
	}
	return re, err
}

func (s *server) MutateRow(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	defer tbl.write()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	now := s.clock()
	r := tbl.getOrCreateRow(req.RowKey)

	if err := applyMutations(tbl, r, req.Mutations, now); err != nil {
		return nil, err
	}
	tbl.updateRow(r)
	return &btpb.MutateRowResponse{}, nil
}

func (s *server) MutateRows(req *btpb.MutateRowsRequest, stream btpb.Bigtable_MutateRowsServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}
	res := &btpb.MutateRowsResponse{Entries: make([]*btpb.MutateRowsResponse_Entry, len(req.Entries))}

	defer tbl.write()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	now := s.clock()

	for i, entry := range req.Entries {
		r := tbl.getOrCreateRow(entry.RowKey)

		code, msg := int32(codes.OK), ""
		if err := applyMutations(tbl, r, entry.Mutations, now); err != nil {
			code = int32(codes.Internal)
			msg = err.Error()
		}
		tbl.updateRow(r)
		res.Entries[i] = &btpb.MutateRowsResponse_Entry{
			Index:  int64(i),
			Status: &statpb.Status{Code: code, Message: msg},
		}
	}
	return stream.Send(res)
}

func (s *server) CheckAndMutateRow(ctx context.Context, req *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}
	res := &btpb.CheckAndMutateRowResponse{}

	defer tbl.write()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	now := s.clock()
	r := tbl.getOrCreateRow(req.RowKey)

	// Figure out which mutation to apply.
	whichMut := false
	if req.PredicateFilter == nil {
		// Use true_mutations iff row contains any cells.
		whichMut = !isEmpty(r)
	} else {
		// Use true_mutations iff any cells in the row match the filter.
		// TODO(dsymonds): This could be cheaper.
		nr := copyRow(r)

		match, err := filterRow(req.PredicateFilter, nr)
		if err != nil {
			return nil, err
		}
		whichMut = match && !isEmpty(nr)
	}
	res.PredicateMatched = whichMut
	muts := req.FalseMutations
	if whichMut {
		muts = req.TrueMutations
	}

	if err := applyMutations(tbl, r, muts, now); err != nil {
		return nil, err
	}
	tbl.updateRow(r)
	return res, nil
}

// applyMutations applies a sequence of mutations to a row.
// It assumes r.mu is locked.
func applyMutations(tbl *table, r *btpb.Row, muts []*btpb.Mutation, now time.Time) error {
	fs := tbl.def.ColumnFamilies
	for _, mut := range muts {
		switch mut := mut.Mutation.(type) {
		default:
			return fmt.Errorf("can't handle mutation type %T", mut)
		case *btpb.Mutation_SetCell_:
			set := mut.SetCell
			if _, ok := fs[set.FamilyName]; !ok {
				return fmt.Errorf("unknown family %q", set.FamilyName)
			}
			ts := set.TimestampMicros
			if ts == -1 { // bigtable.ServerTime
				ts = newTimestamp(now)
			}
			if !tbl.validTimestamp(ts) {
				return fmt.Errorf("invalid timestamp %d", ts)
			}
			fam := set.FamilyName
			col := set.ColumnQualifier

			newCell := &btpb.Cell{TimestampMicros: ts, Value: set.Value}
			f := getOrCreateFamily(r, fam)
			c := getOrCreateColumn(f, col)
			c.Cells = appendOrReplaceCell(c.Cells, newCell)
		case *btpb.Mutation_DeleteFromColumn_:
			del := mut.DeleteFromColumn
			if _, ok := fs[del.FamilyName]; !ok {
				return fmt.Errorf("unknown family %q", del.FamilyName)
			}
			fam := getFamily(r, del.FamilyName)
			if fam == nil {
				break
			}
			col := getColumn(fam, del.ColumnQualifier)
			if col == nil {
				break
			}
			cs := col.Cells
			if del.TimeRange != nil {
				tsr := del.TimeRange
				if !tbl.validTimestamp(tsr.StartTimestampMicros) {
					return fmt.Errorf("invalid timestamp %d", tsr.StartTimestampMicros)
				}
				if !tbl.validTimestamp(tsr.EndTimestampMicros) && tsr.EndTimestampMicros != 0 {
					return fmt.Errorf("invalid timestamp %d", tsr.EndTimestampMicros)
				}
				if tsr.StartTimestampMicros >= tsr.EndTimestampMicros && tsr.EndTimestampMicros != 0 {
					return fmt.Errorf("inverted or invalid timestamp range [%d, %d]", tsr.StartTimestampMicros, tsr.EndTimestampMicros)
				}

				// Find half-open interval to remove.
				// Cells are in descending timestamp order,
				// so the predicates to sort.Search are inverted.
				si, ei := 0, len(cs)
				if tsr.StartTimestampMicros > 0 {
					ei = sort.Search(len(cs), func(i int) bool { return cs[i].TimestampMicros < tsr.StartTimestampMicros })
				}
				if tsr.EndTimestampMicros > 0 {
					si = sort.Search(len(cs), func(i int) bool { return cs[i].TimestampMicros < tsr.EndTimestampMicros })
				}
				if si < ei {
					copy(cs[si:], cs[ei:])
					cs = cs[:len(cs)-(ei-si)]
				}
			} else {
				cs = nil
			}
			col.Cells = cs
		case *btpb.Mutation_DeleteFromRow_:
			r.Families = nil
		case *btpb.Mutation_DeleteFromFamily_:
			if f := getFamily(r, mut.DeleteFromFamily.FamilyName); f != nil {
				f.Columns = nil
			}
		}
	}
	return nil
}

// Remove empty families / columns
func scrubRow(r *btpb.Row, cols map[string]*btapb.ColumnFamily) (*btpb.Row, bool) {
	n := len(r.Families)
	wIdx := 0
	didChange := false
	for _, f := range r.Families {
		cf := cols[f.Name]
		if cf == nil {
			continue
		}
		var changed bool
		f, changed = scrubFam(f)
		didChange = didChange || changed
		if len(f.Columns) > 0 {
			r.Families[wIdx] = f
			wIdx++
		}
	}
	r.Families = r.Families[:wIdx]
	return r, n != wIdx || didChange
}

// Remove empty columns
func scrubFam(f *btpb.Family) (*btpb.Family, bool) {
	n := len(f.Columns)
	wIdx := 0
	for _, c := range f.Columns {
		if len(c.Cells) > 0 {
			f.Columns[wIdx] = c
			wIdx++
		}
	}
	f.Columns = f.Columns[:wIdx]
	sort.Slice(f.Columns, func(i, j int) bool {
		return bytes.Compare(f.Columns[i].Qualifier, f.Columns[j].Qualifier) < 0
	})
	return f, n != wIdx
}

func maxTimestamp(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func newTimestamp(now time.Time) int64 {
	ts := now.UnixNano() / 1e3
	ts -= ts % 1000 // round to millisecond granularity
	return ts
}

func appendOrReplaceCell(cs []*btpb.Cell, newCell *btpb.Cell) []*btpb.Cell {
	replaced := false
	for i, cell := range cs {
		if cell.TimestampMicros == newCell.TimestampMicros {
			cs[i] = newCell
			replaced = true
			break
		}
	}
	if !replaced {
		cs = append(cs, newCell)
	}
	sort.Sort(byDescTS(cs))
	return cs
}

func (s *server) ReadModifyWriteRow(ctx context.Context, req *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	defer tbl.write()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	now := s.clock()
	r := tbl.getOrCreateRow(req.RowKey)
	resultRow := &btpb.Row{Key: req.RowKey} // copy of updated cells
	cols := tbl.cols()

	// Assume all mutations apply to the most recent version of the cell.
	// TODO(dsymonds): Verify this assumption and document it in the proto.
	for _, rule := range req.Rules {
		if _, ok := cols[rule.FamilyName]; !ok {
			return nil, fmt.Errorf("unknown family %q", rule.FamilyName)
		}

		fam := getOrCreateFamily(r, rule.FamilyName)
		col := getOrCreateColumn(fam, rule.ColumnQualifier)
		ts := newTimestamp(now)
		var newCell *btpb.Cell
		var prevVal []byte
		if len(col.Cells) > 0 {
			prevVal = col.Cells[0].Value

			// ts is the max of clock or the prev cell's timestamp in case the
			// prev cell is in the future
			ts = maxTimestamp(ts, col.Cells[0].TimestampMicros)
		}

		switch rule := rule.Rule.(type) {
		default:
			return nil, fmt.Errorf("unknown RMW rule oneof %T", rule)
		case *btpb.ReadModifyWriteRule_AppendValue:
			newCell = &btpb.Cell{TimestampMicros: ts, Value: append(prevVal, rule.AppendValue...)}
		case *btpb.ReadModifyWriteRule_IncrementAmount:
			var v int64
			if prevVal != nil {
				if len(prevVal) != 8 {
					return nil, fmt.Errorf("increment on non-64-bit value")
				}
				v = int64(binary.BigEndian.Uint64(prevVal))
			}
			v += rule.IncrementAmount
			var val [8]byte
			binary.BigEndian.PutUint64(val[:], uint64(v))
			newCell = &btpb.Cell{TimestampMicros: ts, Value: val[:]}
		}

		// Store the new cell
		col.Cells = appendOrReplaceCell(col.Cells, newCell)

		// Store a copy for the result row
		resultFamily := getOrCreateFamily(resultRow, fam.Name)
		resultCol := getOrCreateColumn(resultFamily, col.Qualifier)
		resultCol.Cells = []*btpb.Cell{newCell}
	}

	r, _ = scrubRow(r, cols)
	tbl.rows.ReplaceOrInsert(r)
	resultRow, _ = scrubRow(resultRow, cols)
	return &btpb.ReadModifyWriteRowResponse{Row: resultRow}, nil
}

func (s *server) SampleRowKeys(req *btpb.SampleRowKeysRequest, stream btpb.Bigtable_SampleRowKeysServer) error {
	s.mu.Lock()
	tbl, ok := s.tables[req.TableName]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "table %q not found", req.TableName)
	}

	tbl.mu.RLock()
	defer tbl.mu.RUnlock()

	// The return value of SampleRowKeys is very loosely defined. Return at least the
	// final row key in the table and choose other row keys randomly.
	var offset int64
	var err error
	var lastRow *btpb.Row
	tbl.rows.Ascend(func(r *btpb.Row) bool {
		if rand.Int31n(100) == 0 {
			resp := &btpb.SampleRowKeysResponse{
				RowKey:      r.Key,
				OffsetBytes: offset,
			}
			err = stream.Send(resp)
			if err != nil {
				return false
			}
			lastRow = nil
		} else {
			lastRow = r
		}
		offset += int64(rowsize(r))
		return true
	})
	if err == nil && lastRow != nil {
		resp := &btpb.SampleRowKeysResponse{
			RowKey:      lastRow.Key,
			OffsetBytes: offset - int64(rowsize(lastRow)),
		}
		err = stream.Send(resp)
	}
	return err
}

func (s *server) gcloop() {
	const (
		minWait = 15000 // ms
		maxWait = 60000 // ms
	)

	for {
		// Wait for a random time interval.
		d := time.Duration(minWait+rand.Intn(maxWait-minWait)) * time.Millisecond
		select {
		case <-time.After(d):
		case <-s.done:
			return // server has been closed
		}

		// Do a GC pass over all dirty-but-inactive tables, from oldest modified to newest.
		type todo struct {
			lastWrite int64
			tbl       *table
		}
		var todos []todo
		s.mu.Lock()
		for _, tbl := range s.tables {
			todos = append(todos, todo{atomic.LoadInt64(&tbl.lastWriteNanos), tbl})
		}
		s.mu.Unlock()

		sort.Slice(todos, func(i, j int) bool {
			return todos[i].lastWrite < todos[j].lastWrite
		})

		for _, todo := range todos {
			todo.tbl.gc(s.clock(), s.done, false)
		}
	}
}

type table struct {
	mu   sync.RWMutex
	def  *btapb.Table
	rows Rows // indexed by row key

	lastReadNanos  int64 // atomic, time in nanos on the real system clock
	lastWriteNanos int64 // atomic, time in nanos on the real system clock
}

func newTable(tbl *btapb.Table, rows Rows) *table {
	if tbl.ColumnFamilies == nil {
		tbl.ColumnFamilies = map[string]*btapb.ColumnFamily{}
	}
	realNow := time.Now().UnixNano()
	return &table{
		def:            tbl,
		lastReadNanos:  realNow,
		lastWriteNanos: realNow,
		rows:           rows,
	}
}

func (t *table) cols() map[string]*btapb.ColumnFamily {
	return t.def.ColumnFamilies
}

func (t *table) validTimestamp(ts int64) bool {
	if ts < minValidMilliSeconds || ts > maxValidMilliSeconds {
		return false
	}

	// Assume millisecond granularity is required.
	return ts%1000 == 0
}

// Must hold table lock.
func (t *table) getOrCreateRow(key keyType) *btpb.Row {
	r := t.rows.Get(key)
	if r != nil {
		return r
	}
	return &btpb.Row{Key: key}
}

// Must hold table lock.
func (t *table) updateRow(r *btpb.Row) {
	r, _ = scrubRow(r, t.cols())
	if len(r.Families) == 0 {
		t.rows.Delete(r.Key)
	} else {
		t.rows.ReplaceOrInsert(r)
	}
}

func (t *table) gc(now time.Time, done <-chan struct{}, force bool) {
	if !force {
		// Recheck lastReadNanos/lastWriteNanos
		const quiesceNanos = int64(5 * time.Minute)
		lr := atomic.LoadInt64(&t.lastReadNanos)
		lw := atomic.LoadInt64(&t.lastWriteNanos)
		realNow := time.Now().UnixNano()
		if lw == 0 || realNow-lw < quiesceNanos || realNow-lr < quiesceNanos {
			return
		}
	}

	defer atomic.StoreInt64(&t.lastWriteNanos, 0) // mark GC done

	// TODO(scottb): if the table is still idle after GC is done, send Rows a CloseHint()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Gather GC rules we'll apply.
	rules := make(map[string]*btapb.GcRule) // keyed by "fam"
	for fam, cf := range t.cols() {
		if cf.GcRule != nil {
			rules[fam] = cf.GcRule
		}
	}
	if len(rules) == 0 {
		return
	}

	// TODO(scottb): could collect batches of rows that need GC with only a read lock, update with write lock.

	i := 0
	t.rows.Ascend(func(r *btpb.Row) bool {
		changed := false
		for _, fam := range r.Families {
			gcRule := rules[fam.Name]
			if gcRule != nil {
				for _, col := range fam.Columns {
					n := len(col.Cells)
					col.Cells = applyGC(col.Cells, gcRule, now)
					changed = changed || n != len(col.Cells)
				}
			}
		}
		if changed {
			r, _ := scrubRow(r, t.cols())
			t.rows.ReplaceOrInsert(r)
		}
		i++
		if i%100 != 0 {
			return true
		}

		// Reverse lock; check if we should exit
		t.mu.Unlock()
		defer t.mu.Lock()
		select {
		case <-done:
			return false // server has been closed
		default:
			return true
		}
	})
}

func (t *table) read() {
	now := time.Now().UnixNano()
	for {
		old := atomic.LoadInt64(&t.lastReadNanos)
		if now < old {
			return
		}
		if atomic.CompareAndSwapInt64(&t.lastReadNanos, old, now) {
			return
		}
	}
}

func (t *table) write() {
	now := time.Now().UnixNano()
	for {
		old := atomic.LoadInt64(&t.lastWriteNanos)
		if now < old {
			return
		}
		if atomic.CompareAndSwapInt64(&t.lastWriteNanos, old, now) {
			return
		}
	}
}

// copy returns a copy of the row.
// Cell values are aliased.
func copyRow(r *btpb.Row) *btpb.Row {
	nr := &btpb.Row{Key: r.Key}
	for _, fam := range r.Families {
		f := &btpb.Family{
			Name: fam.Name,
		}
		for _, col := range fam.Columns {
			// Copy the cell slice, but not the []byte inside each cell.
			f.Columns = append(f.Columns, &btpb.Column{
				Qualifier: col.Qualifier,
				Cells:     append([]*btpb.Cell{}, col.Cells...),
			})
		}
		nr.Families = append(nr.Families, f)
	}
	return nr
}

// isEmpty returns true if a row doesn't contain any cell
func isEmpty(r *btpb.Row) bool {
	for _, fam := range r.Families {
		for _, cs := range fam.Columns {
			if len(cs.Cells) > 0 {
				return false
			}
		}
	}
	return true
}

func getFamily(r *btpb.Row, name string) *btpb.Family {
	for _, fam := range r.Families {
		if fam.Name == name {
			return fam
		}
	}
	return nil
}

func getOrCreateFamily(r *btpb.Row, name string) *btpb.Family {
	if fam := getFamily(r, name); fam != nil {
		return fam
	}
	fam := &btpb.Family{Name: name}
	r.Families = append(r.Families, fam)
	return fam
}

// rowsize returns the total size of all cell values in the row.
func rowsize(r *btpb.Row) int {
	size := 0
	for _, fam := range r.Families {
		for _, col := range fam.Columns {
			for _, cell := range col.Cells {
				size += len(cell.Value)
			}
		}
	}
	return size
}

var gcTypeWarn sync.Once

// applyGC applies the given GC rule to the cells.
func applyGC(cells []*btpb.Cell, rule *btapb.GcRule, now time.Time) []*btpb.Cell {
	switch rule := rule.Rule.(type) {
	default:
		// TODO(dsymonds): Support GcRule_Intersection_
		gcTypeWarn.Do(func() {
			log.Printf("Unsupported GC rule type %T", rule)
		})
	case *btapb.GcRule_Union_:
		for _, sub := range rule.Union.Rules {
			cells = applyGC(cells, sub, now)
		}
		return cells
	case *btapb.GcRule_MaxAge:
		// Timestamps are in microseconds.
		cutoff := now.UnixNano() / 1e3
		cutoff -= rule.MaxAge.Seconds * 1e6
		cutoff -= int64(rule.MaxAge.Nanos) / 1e3
		// The slice of cells in in descending timestamp order.
		// This sort.Search will return the index of the first cell whose timestamp is chronologically before the cutoff.
		si := sort.Search(len(cells), func(i int) bool { return cells[i].TimestampMicros < cutoff })
		if si < len(cells) {
			log.Printf("bttest: GC MaxAge(%v) deleted %d cells.", rule.MaxAge, len(cells)-si)
		}
		return cells[:si]
	case *btapb.GcRule_MaxNumVersions:
		n := int(rule.MaxNumVersions)
		if len(cells) > n {
			cells = cells[:n]
		}
		return cells
	}
	return cells
}

func getColumn(fam *btpb.Family, name []byte) *btpb.Column {
	for _, col := range fam.Columns {
		if bytes.Equal(col.Qualifier, name) {
			return col
		}
	}
	return nil
}

func getOrCreateColumn(fam *btpb.Family, name []byte) *btpb.Column {
	if col := getColumn(fam, name); col != nil {
		return col
	}
	col := &btpb.Column{Qualifier: name}
	fam.Columns = append(fam.Columns, col)
	return col
}

type byDescTS []*btpb.Cell

func (b byDescTS) Len() int           { return len(b) }
func (b byDescTS) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byDescTS) Less(i, j int) bool { return b[i].TimestampMicros > b[j].TimestampMicros }
