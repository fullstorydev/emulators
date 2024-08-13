package bttest

import (
	"bytes"

	btapb "cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/btree"
	"google.golang.org/protobuf/proto"
)

const btreeDegree = 16

// BtreeStorage stores data in an in-memory btree. This implementation is here for historical reference
// and should not generally be used; prefer LeveldbMemStorage. BtreeStorage's row scans do not work well
// in the face of concurrent insertions and deletions. Although no data races occur, changes to the Btree's
// internal structure break iteration in surprising ways, resulting in unpredictable rowscan results.
type BtreeStorage struct {
}

var _ Storage = BtreeStorage{}

// Create a new table, destroying any existing table.
func (BtreeStorage) Create(_ *btapb.Table) Rows {
	return btreeRows{btree.New(btreeDegree)}
}

// GetTables returns metadata about all stored tables.
func (BtreeStorage) GetTables() []*btapb.Table {
	return nil
}

// Open the given table, which must have been previously returned by GetTables().
func (BtreeStorage) Open(_ *btapb.Table) Rows {
	panic("should not get here")
}

// SetTableMeta persists metadata about a table.
func (f BtreeStorage) SetTableMeta(_ *btapb.Table) {
}

type btreeRows struct {
	tree *btree.BTree
}

var _ Rows = btreeRows{}

func (b btreeRows) Ascend(iterator RowIterator) {
	b.tree.Ascend(b.adaptIterator(iterator))
}

func (b btreeRows) AscendRange(greaterOrEqual, lessThan keyType, iterator RowIterator) {
	b.tree.AscendRange(b.key(greaterOrEqual), b.key(lessThan), b.adaptIterator(iterator))
}

func (b btreeRows) AscendLessThan(lessThan keyType, iterator RowIterator) {
	b.tree.AscendLessThan(b.key(lessThan), b.adaptIterator(iterator))
}

func (b btreeRows) AscendGreaterOrEqual(greaterOrEqual keyType, iterator RowIterator) {
	b.tree.AscendGreaterOrEqual(b.key(greaterOrEqual), b.adaptIterator(iterator))
}

func (b btreeRows) Delete(key keyType) {
	b.tree.Delete(b.key(key))
}

func (b btreeRows) Get(key keyType) *btpb.Row {
	item := b.tree.Get(b.key(key))
	if item == nil {
		return nil
	}
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) ReplaceOrInsert(r *btpb.Row) {
	b.tree.ReplaceOrInsert(protoItem{
		key: r.Key,
		buf: toProto(r),
	})
}

func (b btreeRows) Clear() {
	b.tree.Clear(false)
}

func (b btreeRows) Close() {
}

func (b btreeRows) key(key keyType) protoItem {
	return protoItem{key: key}
}

func (b btreeRows) adaptIterator(iterator RowIterator) btree.ItemIterator {
	return func(i btree.Item) bool {
		r := fromProto(i.(protoItem).buf)
		return iterator(r)
	}
}

func fromProto(buf []byte) *btpb.Row {
	var p btpb.Row
	if err := proto.Unmarshal(buf, &p); err != nil {
		panic(err)
	}
	return &p
}

func toProto(r *btpb.Row) []byte {
	if buf, err := proto.Marshal(r); err != nil {
		panic(err)
	} else {
		return buf
	}
}

type protoItem struct {
	key keyType
	buf []byte
}

var _ btree.Item = protoItem{}

// Less implements btree.Item.
func (bi protoItem) Less(i btree.Item) bool {
	return bytes.Compare(bi.key, i.(protoItem).key) < 0
}
