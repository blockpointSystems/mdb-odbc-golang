package mdb

import "database/sql/driver"

func (db *Conn) IsClosed() bool {
	if db != nil {
		//panic("Implement me!")
		return db.closed
	}
	return true
}

func (db *Conn) SetClosed() {
	// TODO: Make atomic
	db.closed = true
}

func (db *Conn) markBadConn(err error) error {
	if db == nil {
		return err
	}
	if err != errBadConnNoWrite {
		return err
	}
	return driver.ErrBadConn
}