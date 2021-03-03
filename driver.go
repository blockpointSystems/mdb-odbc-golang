package mdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// MDBDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type MDBDriver struct {}

func (mdb *MDBDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func init() {
	sql.Register("mdb", &MDBDriver{})
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	cfg = cfg.Clone()
	// normalize the contents of cfg so calls to NewConnector have the same
	// behavior as MDBDriver.OpenConnector
	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	return &connector{cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext.
func (d MDBDriver) OpenConnector(dsn string) (conn driver.Connector, err error) {
	var cfg *Config
	cfg, err = ParseDSN(dsn)
	if err != nil {
		return
	}

	conn = &connector{
		cfg: cfg,
	}
	return
}
