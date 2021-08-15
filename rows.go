package mdb

import (
	"database/sql/driver"
	"github.com/blockpointSystems/protocol-buffers/v1/odbc"
	"io"
	"reflect"
	"sync/atomic"
)

type resultSet struct {
	columnNames []string
	rows        [][]driver.Value
}

// Rows is an iterator over an executed query's results.
type Rows struct {
	streamRecv *odbc.MDBService_QueryClient

	schema *odbc.Schema

	set     resultSet
	nextSet *odbc.QueryResponse

	setPos int32

	close func() error
	done  bool
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	if r != nil {
		return r.set.columnNames
	}
	return []string{}
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	if !r.done {
		panic("not done")
	}
	r.schema   = nil

	r.set.columnNames = nil
	r.set.rows 		  = nil

	r.nextSet = nil
	return r.close()
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) (err error) {
	pos := atomic.AddInt32(&r.setPos, 1) - 1
	if int(pos) < len(r.set.rows) {
		copy(dest, r.set.rows[pos])
		return
	}

	err = r.NextResultSet()
	if err != nil {
		return
	}

	return r.Next(dest)
}


// RowsNextResultSet extends the Rows interface by providing a way to signal
// the driver to advance to the next result set.
//type RowsNextResultSet interface {}

// HasNextResultSet is called at the end of the current result set and
// reports whether there is another result set after the current one.
func (r *Rows) HasNextResultSet() bool {
	if r.nextSet != nil {
		return true
	}

	nextResp, err := (*r.streamRecv).Recv()
	if err != nil {
		return false
	}

	r.nextSet = nextResp
	return true
}

// NextResultSet advances the driver to the next result set even
// if there are remaining rows in the current result set.
//
// NextResultSet should return io.EOF when there are no more result sets.
func (r *Rows) NextResultSet() error {
	if r.HasNextResultSet() {
		// Build the next result set
		r.set.buildNextResultSet(r.nextSet.GetRespSchema(), r.nextSet.GetResultSet())
		r.done = r.nextSet.GetDone()

		// Clear the nextSet queue as it has been bumped up
		atomic.StoreInt32(&r.setPos, 0)
		r.nextSet = nil

		// Return nil
		return nil
	}
	return io.EOF
}

// RowsColumnTypeScanType may be implemented by Rows. It should return
// the value type that can be used to scan types into. For example, the database
// column type "bigint" this should return "reflect.TypeOf(int64(0))".
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	switch r.schema.GetColumnType()[index] {
	case odbc.Datatype_BYTEARRAY:
		return scanTypeRawBytes
	case odbc.Datatype_STRING:
		return scanTypeString
	case odbc.Datatype_INT8:
		return scanTypeInt8
	case odbc.Datatype_UINT8:
		return scanTypeUint8
	case odbc.Datatype_INT16:
		return scanTypeInt16
	case odbc.Datatype_UINT16:
		return scanTypeUint16
	case odbc.Datatype_INT32:
		return scanTypeInt32
	case odbc.Datatype_UINT32:
		return scanTypeUint32
	case odbc.Datatype_INT64:
		return scanTypeInt64
	case odbc.Datatype_UINT64:
		return scanTypeUint64
	case odbc.Datatype_FLOAT32:
		return scanTypeFloat32
	case odbc.Datatype_FLOAT64:
		return scanTypeFloat64
	//case odbc.Datatype_COMPLEX64:
	//	panic("not supported")
	//case odbc.Datatype_COMPLEX128:
	//	panic("not supported")
	case odbc.Datatype_BOOL:
		return scanTypeBoolean
	case odbc.Datatype_TIMESTAMP:
		return scanTypeNullTime
	case odbc.Datatype_UUID:
		return scanTypeString
	}
	return nil
}

// RowsColumnTypeDatabaseTypeName may be implemented by Rows. It should return the
// database system type name without the length. Type names should be uppercase.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	switch r.schema.GetColumnType()[index] {
	case odbc.Datatype_BYTEARRAY:
		return "BYTEARRAY"
	case odbc.Datatype_STRING:
		return "STRING"
	case odbc.Datatype_INT8:
		return "INT8"
	case odbc.Datatype_UINT8:
		return "UINT8"
	case odbc.Datatype_INT16:
		return "INT16"
	case odbc.Datatype_UINT16:
		return "UINT16"
	case odbc.Datatype_INT32:
		return "INT32"
	case odbc.Datatype_UINT32:
		return "UINT32"
	case odbc.Datatype_INT64:
		return "INT64"
	case odbc.Datatype_UINT64:
		return "UINT64"
	case odbc.Datatype_FLOAT32:
		return "FLOAT32"
	case odbc.Datatype_FLOAT64:
		return "FLOAT64"
	//case odbc.Datatype_COMPLEX64:
	//	panic("not supported")
	//case odbc.Datatype_COMPLEX128:
	//	panic("not supported")
	case odbc.Datatype_BOOL:
		return "BOOL"
	case odbc.Datatype_TIMESTAMP:
		return "TIMESTAMP"
	case odbc.Datatype_UUID:
		return "UUID"
	}
	return "UNDEF"
}

//// RowsColumnTypeLength may be implemented by Rows. It should return the length
//// of the column type if the column is a variable length type. If the column is
//// not a variable length type ok should return false.
//// If length is not limited other than system limits, it should return math.MaxInt64.
//// The following are examples of returned values for various types:
////   TEXT          (math.MaxInt64, true)
////   varchar(10)   (10, true)
////   nvarchar(10)  (10, true)
////   decimal       (0, false)
////   int           (0, false)
////   bytea(30)     (30, true)
//func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
//	length = r.schema.GetColumnSize()[index]
//	switch r.schema.GetColumnType()[index] {
//	case odbc.Datatype_BYTEARRAY, odbc.Datatype_STRING:
//		ok = true
//	}
//	return
//}

//// RowsColumnTypeNullable may be implemented by Rows. The nullable value should
//// be true if it is known the column may be null, or false if the column is known
//// to be not nullable.
//// If the column nullability is unknown, ok should be false.
//func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
//	panic("implement me!")
//	if r != nil {
//		bit, err := GetBitFromBytes(r.schema.GetColumnIsNullableBitmap(), index)
//		if err != nil {
//			return
//		}
//		nullable = bit == ONE
//		ok = true
//	}
//	return
//}

// RowsColumnTypePrecisionScale may be implemented by Rows. It should return
// the precision and scale for decimal types. If not applicable, ok should be false.
// The following are examples of returned values for various types:
//   decimal(38, 4)    (38, 4, true)
//   int               (0, 0, false)
//   decimal           (math.MaxInt64, math.MaxInt64, true)
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	// TODO: CHECK IMPLEMENTATION
	switch r.schema.GetColumnType()[index] {
	case odbc.Datatype_FLOAT32:
		precision = 11
		scale     = 2
		ok		  = true
	case odbc.Datatype_FLOAT64:
		precision = 11
		scale     = 2
		ok		  = true
	}
	return
}

func (rs *resultSet) buildNextResultSet(schema *odbc.Schema, set []*odbc.Row) {
	rs.rows = make([][]driver.Value, len(set))

	for i, row := range set {
		rs.rows[i] = make([]driver.Value, len(rs.columnNames))
		for j, col := range row.GetColumns() {

			bit, _ := GetBitFromBytes(row.GetNullColumnBitmap(), j)

			if bit == ONE {
				// Column is null
				continue
			}
			// Column is not null
			rs.rows[i][j] = convertColumnToValue(col, schema.GetColumnType()[j])
		}
	}

	return
}