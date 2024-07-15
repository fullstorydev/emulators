package bttest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/opt"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	"google.golang.org/protobuf/proto"
)

// LeveldbDiskStorage stores data persistently on leveldb.
type LeveldbDiskStorage struct {
	// A root directory under which all data is stored.
	Root string

	// Optional error logger.
	ErrLog func(err error, msg string)

	// TODO: options like compression?
}

// Create a new table, destroying any existing table.
func (f LeveldbDiskStorage) Create(tbl *btapb.Table) Rows {
	f.SetTableMeta(tbl)
	path := filepath.Join(f.Root, tbl.Name)
	newFunc := func(nuke bool) *leveldb.DB {
		return newDiskDb(path, nuke)
	}

	return &leveldbRows{
		db:      newFunc(true),
		newFunc: newFunc,
	}
}

// GetTables returns metadata about all stored tables.
func (f LeveldbDiskStorage) GetTables() []*btapb.Table {
	// Ignore any errors, just return
	var ret []*btapb.Table
	err := filepath.Walk(f.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, ".table.proto") {
			return nil
		}
		var tbl btapb.Table
		buf, err := os.ReadFile(path)
		if err != nil {
			f.errLog(err, "openq %q", path)
			return nil
		}
		if err := proto.Unmarshal(buf, &tbl); err != nil {
			f.errLog(err, "unmarshal %q", path)
			return nil
		}
		ret = append(ret, &tbl)
		return nil
	})
	if err != nil {
		f.errLog(err, "walk %q", f.Root)
	}
	return ret
}

// Open the given table, which must have been previously returned by GetTables().
func (f LeveldbDiskStorage) Open(tbl *btapb.Table) Rows {
	path := filepath.Join(f.Root, tbl.Name)
	newFunc := func(nuke bool) *leveldb.DB {
		return newDiskDb(path, nuke)
	}

	return &leveldbRows{
		db:      newFunc(false),
		newFunc: newFunc,
	}
}

// SetTableMeta persists metadata about a table.
func (f LeveldbDiskStorage) SetTableMeta(tbl *btapb.Table) {
	path := filepath.Join(f.Root, tbl.Name)
	if err := os.MkdirAll(path, 0777); err != nil {
		f.errLog(err, "os.MkdirAll %q", path)
	}
	buf, err := proto.Marshal(tbl)
	if err != nil {
		panic(err) // should not fail
	}

	outPath := filepath.Join(path + ".table.proto")
	tmpPath := filepath.Join(path + ".table.proto.tmp")
	if err := os.WriteFile(tmpPath, buf, 0666); err != nil {
		f.errLog(err, "ioutil.WriteFile %q", tmpPath)
		return
	}

	if err := os.Rename(tmpPath, outPath); err != nil {
		f.errLog(err, "os.Rename %q -> %q", tmpPath, outPath)
		return
	}
}

func (f LeveldbDiskStorage) errLog(err error, format string, args ...interface{}) {
	if f.ErrLog != nil {
		f.ErrLog(err, fmt.Sprintf(format, args...))
	}
}

var _ Storage = LeveldbDiskStorage{}

func newDiskDb(path string, nuke bool) *leveldb.DB {
	if nuke {
		_ = os.RemoveAll(path)
	}

	db, err := leveldb.OpenFile(path, &opt.Options{
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
