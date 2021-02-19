package mdb

import "database/sql/driver"

// Tx is a transaction.
type Tx struct {
	*Conn

	id uint64
	driver.TxOptions
}


func CreateTransaction(xactId uint64, xactCfg driver.TxOptions, dbConn *Conn) Tx {
	return Tx{
		Conn: 	   dbConn,
		id:   	   xactId,
		TxOptions: xactCfg,
	}
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
