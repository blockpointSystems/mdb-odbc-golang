package mdb

import "database/sql/driver"

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	conn *Conn

	stmt 	   string
	paramCount int
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() (err error) {
	if s.conn == nil || s.conn.IsClosed() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		return driver.ErrBadConn
	}

	err = s.conn.Close()
	s.conn = nil
	return err
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	return s.paramCount
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.Exec(s.stmt, args)
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.Query(s.stmt, args)
}