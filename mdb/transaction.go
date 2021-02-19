package mdb

// Tx is a transaction.
type Tx struct {
	*Conn
}



func (xact Tx) Commit() (err error) {
	if xact.Conn != nil && !xact.IsClosed() {
		_, _, err = xact.exec("COMMIT")
		xact.Conn = nil
		return
	}
	return ErrInvalidConn
}

func (xact Tx) Rollback() (err error) {
	if xact.Conn != nil && !xact.IsClosed() {
		_, _, err = xact.exec("ROLLBACK")
		return
	}
	return ErrInvalidConn
}
