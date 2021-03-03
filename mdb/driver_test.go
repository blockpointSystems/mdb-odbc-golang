package mdb

import (
	"context"
	"database/sql/driver"
	"log"
	"testing"
)

func TestBasicDriverUsage(t *testing.T) {
	var (
		d MDBDriver

		connector driver.Connector
		conn 	  driver.Conn

		stmt driver.Stmt
		//tx   driver.Tx
		rows driver.Rows

		respRow []driver.Value

		err error
	)

	connector, err = d.OpenConnector("system:biglove@tcp(0.0.0.0:8080)/main")
	if err != nil {
		t.Error(err)
	}

	conn, err = connector.Connect(context.Background())
	if err != nil {
		t.Error(err)
	}

	stmt, err = conn.Prepare("SELECT * FROM main.user")
	if err != nil {
		t.Error(err)
	}

	rows, err = stmt.Query([]driver.Value{})
	if err != nil {
		t.Error(err)
	}

	respRow = make([]driver.Value, len(rows.Columns()))
	err = rows.Next(respRow)
	for err == nil {
		log.Print(respRow)
		err = rows.Next(respRow)
	}



	//tx, err = conn.Begin()
	//
	//conn.Prepare()
	//
	//tx.Commit()


	//sql.
}
