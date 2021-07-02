package mdb

import (
	"database/sql/driver"
	"sync/atomic"
)

func (db *Conn) IsClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

func (db *Conn) SetClosed() {
	atomic.StoreUint32(&db.closed, 1)
}

func (db *Conn) SetNotClosed() {
	atomic.StoreUint32(&db.closed, 0)
}

func (db *Conn) IsActiveQuery() bool {
	return atomic.LoadUint32(&db.activeQuery) != 0
}

func (db *Conn) SetActiveQuery() {
	atomic.StoreUint32(&db.activeQuery, 1)
}

func (db *Conn) SetNotActiveQuery() {
	atomic.StoreUint32(&db.activeQuery, 0)
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
