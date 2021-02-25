package mdb

import (
	"context"
	"database/sql/driver"
)

type connector struct {
	cfg *Config // immutable private copy.
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (conn driver.Conn, err error) {


	return
}

// Driver implements driver.Connector interface.
// Driver returns &MDBDriver{}.
func (c *connector) Driver() driver.Driver {
	return &MDBDriver{}
}