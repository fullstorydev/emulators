package bttest

import "cloud.google.com/go/bigtable"

// GServer is the server implementation of bigtable.
type GServer = server

// NewGServer creates a new GServer.
func NewGServer() *GServer {
	return NewGServerWithOptions(Options{})
}

// NewGServerWithOptions creates a new GServer with the given options.
// GrpcOpts is ignored; this is for creating your own gRPC server.
func NewGServerWithOptions(opt Options) *GServer {
	if opt.Storage == nil {
		opt.Storage = LeveldbMemStorage{}
	}
	if opt.Clock == nil {
		opt.Clock = bigtable.Now
	}
	s := &server{
		storage: opt.Storage,
		tables:  make(map[string]*table),
		clock:   opt.Clock,
		done:    make(chan struct{}),
	}

	// Init from storage.
	for _, tbl := range s.storage.GetTables() {
		rows := s.storage.Open(tbl)
		s.tables[tbl.Name] = newTable(tbl, rows)
	}

	return s
}

// Close shuts down the server.
func (s *GServer) Close() {
	close(s.done)

	var tbls []*table
	s.mu.Lock()
	for _, t := range s.tables {
		tbls = append(tbls, t)
	}
	s.mu.Unlock()

	for _, tbl := range tbls {
		func() {
			tbl.mu.Lock()
			defer tbl.mu.Unlock()
			tbl.rows.Close()
		}()
	}
}
