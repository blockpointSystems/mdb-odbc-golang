package mdb

import (
	"context"
	"database/sql/driver"
	"gitlab.com/blockpoint/mdb-odbc-golang/protocolBuffers/odbc"
	"google.golang.org/grpc"
)

type connector struct {
	cfg *Config // immutable private copy.
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (conn driver.Conn, err error) {
	var (
		mdbConn  *Conn
		grpcConn *grpc.ClientConn
	)

	grpcConn, err = grpc.Dial(c.cfg.Addr, grpc.WithInsecure())
	if err != nil {
		return
	}

	mdbConn = &Conn{
		cfg:              c.cfg,
	  //status:           0,

		MDBServiceClient: odbc.NewMDBServiceClient(grpcConn),
	}

	err = mdbConn.configureConnection()
	if err != nil {
		// Close the connection and return
		grpcConn.Close()
		return
	}

	conn = mdbConn
	return
}

// Driver implements driver.Connector interface.
// Driver returns &MDBDriver{}.
func (c *connector) Driver() driver.Driver {
	return &MDBDriver{}
}