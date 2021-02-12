package custom

// Conn is a connection to a database. It is not used concurrently
// by multiple goroutines.
//
// Conn is assumed to be stateful.
type Conn struct {

}
// Prepare returns a prepared statement, bound to this connection.
func (db *Conn)	Prepare(query string) (s Stmt, err error) {
	panic("implement me")
	return
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (db *Conn)	Close() (err error) {
	panic("implement me")
	return
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (db *Conn)	Begin() (xact Tx, err error) {
	panic("implement me")
	return
}

