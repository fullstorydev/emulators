package bttest

import (
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type leveldbRows struct {
	db      *leveldb.DB
	newFunc func(nuke bool) *leveldb.DB
}

var _ Rows = &leveldbRows{}

func (rows *leveldbRows) Ascend(iterator RowIterator) {
	rows.ascendRange(nil, iterator)
}

func (rows *leveldbRows) AscendRange(greaterOrEqual, lessThan keyType, iterator RowIterator) {
	rows.ascendRange(&util.Range{
		Start: greaterOrEqual,
		Limit: lessThan,
	}, iterator)
}

func (rows *leveldbRows) AscendLessThan(lessThan keyType, iterator RowIterator) {
	rows.ascendRange(&util.Range{
		Limit: lessThan,
	}, iterator)
}

func (rows *leveldbRows) AscendGreaterOrEqual(greaterOrEqual keyType, iterator RowIterator) {
	rows.ascendRange(&util.Range{
		Start: greaterOrEqual,
	}, iterator)
}

func (rows *leveldbRows) Delete(key keyType) {
	err := rows.db.Delete(key, nil)
	if err != nil {
		panic(err)
	}
}

func (rows *leveldbRows) Get(key keyType) *btpb.Row {
	item, err := rows.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil
	} else if err != nil {
		panic(err)
	}
	return fromProto(item)
}

func (rows *leveldbRows) ReplaceOrInsert(r *btpb.Row) {
	err := rows.db.Put(r.Key, toProto(r), nil)
	if err != nil {
		panic(err)
	}
}

func (rows *leveldbRows) Clear() {
	if err := rows.db.Close(); err != nil {
		panic(err)
	}
	rows.db = rows.newFunc(true)
}

func (rows *leveldbRows) Close() {
	if err := rows.db.Close(); err != nil {
		panic(err)
	}
}

func (rows *leveldbRows) ascendRange(rng *util.Range, iterator RowIterator) {
	it := rows.db.NewIterator(rng, nil)
	defer it.Release()
	for ok := it.First(); ok; ok = it.Next() {
		iterator(fromProto(it.Value()))
	}
	if err := it.Error(); err != nil {
		panic(err)
	}
}
