package bttest

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
)

// LeveldbMemStorage stores data in an in-memory level db. This is the default.
// Unlike BtreeStorage, LeveldbMemStorage is resilient against concurrent insertions and deletions during
// row scans. Concurrently added and deleted rows may or may be scanned (as with real bigtable), but the
// general row scan semantics should hold.
type LeveldbMemStorage struct {
}

func (f LeveldbMemStorage) Create(_ *btapb.Table) Rows {
	newFunc := func(nuke bool) *leveldb.DB {
		return newMemDb(nuke)
	}
	return &leveldbRows{
		db:      newFunc(false),
		newFunc: newFunc,
	}
}

func (f LeveldbMemStorage) GetTables() []*btapb.Table {
	return nil
}

func (f LeveldbMemStorage) Open(_ *btapb.Table) Rows {
	panic("should not get here")
}

func (f LeveldbMemStorage) SetTableMeta(_ *btapb.Table) {
}

var _ Storage = LeveldbMemStorage{}

func newMemDb(_ bool) *leveldb.DB {
	db, err := leveldb.Open(storage.NewMemStorage(), &opt.Options{
		Comparer:                     comparer.DefaultComparer,
		Compression:                  opt.NoCompression,
		DisableBufferPool:            true,
		DisableLargeBatchTransaction: true,
	})
	if err != nil {
		panic(err)
	}
	return db
}
