package bttest

import (
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// Storage implements a storage layer for all bigtable emulator data.
type Storage interface {
	// Create a new table, destroying any existing table.
	Create(tbl *btapb.Table) Rows
	// GetTables returns metadata about all stored tables.
	GetTables() []*btapb.Table
	// Open the given table, which must have been previously returned by GetTables().
	Open(tbl *btapb.Table) Rows
	// SetTableMeta persists metadata about a table.
	SetTableMeta(tbl *btapb.Table)
}

type keyType = []byte

// Rows implements storage algorithms per table.
type Rows interface {
	// Ascend calls the iterator for every row in the table within the range
	// [first, last], until iterator returns false.
	Ascend(iterator RowIterator)

	// AscendRange calls the iterator for every row in the table within the range
	// [greaterOrEqual, lessThan), until iterator returns false.
	AscendRange(greaterOrEqual, lessThan keyType, iterator RowIterator)

	// AscendLessThan calls the iterator for every row in the table within the range
	// [first, pivot), until iterator returns false.
	AscendLessThan(lessThan keyType, iterator RowIterator)

	// AscendGreaterOrEqual calls the iterator for every row in the table within
	// the range [pivot, last], until iterator returns false.
	AscendGreaterOrEqual(greaterOrEqual keyType, iterator RowIterator)

	// Clear removes all rows from the table.
	Clear()

	// Delete removes a row whose key is equal to given key.
	Delete(key keyType)

	// Get looks for a row whose key is equal to the given key, returning it.
	// Returns nil if unable to find that row.
	Get(key keyType) *btpb.Row

	// ReplaceOrInsert adds the given row to the table.  If a row in the table
	// already equals the given one, it is removed from the table.
	//
	// nil cannot be added to the table (will panic).
	ReplaceOrInsert(r *btpb.Row)

	Close()
}

// RowIterator is a callback function that receives a Row.
type RowIterator = func(r *btpb.Row) bool
