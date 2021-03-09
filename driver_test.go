package mdb

import (
	"context"
	"database/sql"
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

	conn.Close()

	//tx, err = conn.Begin()
	//
	//conn.Prepare()
	//
	//tx.Commit()


	//sql.
}

func TestBasicSQLImplementation(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		user struct{
			id 		  uint64 `db:"id"`
			firstName string
			lastName  string
			age 	  uint8
			username  string
		}
		//resp interface{}
	)
	//sql.Register("mdb", &MDBDriver{})
	mdb, err = sql.Open("mdb", "g")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT id, first_name, last_name, age, username FROM user")
	checkErr(t, mdb, err)

	for rows.Next() {
		err = rows.Scan(&user.id, &user.firstName, &user.lastName, &user.age, &user.username)
		checkErr(t, mdb, err)

		log.Printf("USER: %v\n", user)
	}

	//for
}

func checkErr(t *testing.T, mdb *sql.DB, err error) {
	if err != nil {
		mdb.Close()
		t.Error(err)
		panic("")
	}
}
