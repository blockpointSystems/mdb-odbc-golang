package mdb

import "database/sql/driver"

type MDBDriver struct {

}

func (mdb *MDBDriver) Open(name string) (c driver.Conn, err error) {
	return
}
