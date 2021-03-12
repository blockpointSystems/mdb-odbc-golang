package mdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"testing"
	"time"
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

}

func TestBasicSQLImplementation(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		command struct{
			id 		  string `db:"id"`
			xactId    uint64
			status    bool
			startTime 	  time.Time
			endTime  		time.Time
			duration float64
			username string
			commandType uint8
			SQLtext string
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM sys_sessions")
	checkErr(t, mdb, err)


	for rows.Next() {
		err = rows.Scan(
			&command.id,
			&command.xactId,
			&command.status,
			&command.startTime,
			&command.endTime,
			&command.duration,
			&command.username,
			&command.commandType,
			&command.SQLtext,
			)
		checkErr(t, mdb, err)

		log.Printf("command: %v\n", command)
	}
	
	mdb.Close()
}

// Test EXEC...
func TestBasicExecImplementation(t *testing.T) {
	var (
		mdb *sql.DB
		err error
		result sql.Result
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")
	checkErr(t, mdb, err)

	result, err = mdb.Exec("CREATE DATABASE users")
	checkErr(t, mdb, err)

	handleResult(t, mdb, result)

	result, err = mdb.Exec("USE users")
	checkErr(t, mdb, err)

	handleResult(t, mdb, result)

	result, err = mdb.Exec("CREATE BLOCKCHAIN user TRADITIONAL (id uint64 PRIMARY KEY AUTO INCREMENT, first_name string size = 25 PACKED, last_name string size = 50 PACKED, age uint8, username string size=30 PACKED UNIQUE)")
	checkErr(t, mdb, err)

	handleResult(t, mdb, result)

	result, err = mdb.Exec("INSERT user (first_name, last_name, age, username) VALUES (\"Paul\", \"Smith\", 20, \"pdawgy\")")
	checkErr(t, mdb, err)

	lastInsert, rowsAffected := handleResult(t, mdb, result)
	if lastInsert != int64(0) {
		mdb.Close()
		panic("")
	}
	if rowsAffected != int64(1) {
		mdb.Close()
		panic("")
	}

	result, err = mdb.Exec("INSERT user (first_name, last_name, age, username) VALUES (\"Cassidy\", \"Smith\", 23, \"rolly\"), (\"Cassidy\", \"Smith\", 23, \"rollyPolly\")")
	checkErr(t, mdb, err)

	lastInsert, rowsAffected = handleResult(t, mdb, result)
	if lastInsert != int64(2) {
		mdb.Close()
		panic("")
	}
	if rowsAffected != int64(2) {
		mdb.Close()
		panic("")
	}

	mdb.Close()
}


func TestBasicBegin(t *testing.T) {
	var (
		mdb *sql.DB
		err error
		xact *sql.Tx

		rows *sql.Rows

		user struct{
			id 		  uint64 `db:"id"`
			firstName,
			lastName  string
			age 	  uint8
			username  		string
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/main")
	checkErr(t, mdb, err)

	xact, err = mdb.Begin()
	checkErr(t, mdb, err)

	// Run a query that will error.
	_, err = xact.Exec("INSERT user (whipper_snapper, last_name, age, username) VALUES (\"Paul\", \"Smith\", 20, \"pdawgy\")")
	if err != nil {
		if rollbackErr := xact.Rollback(); rollbackErr != nil {
			log.Fatal(rollbackErr)
		}
		err = nil
	}

	// start another transaction
	xact, err = mdb.Begin()
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM user")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM user")
	checkErr(t, mdb, err)

	// This should work fine.
	_, err = xact.Exec("INSERT user (first_name, last_name, age, username) VALUES (\"it's CHABOY\", \"Smith\", 45, \"CHABOY\")")
	checkErr(t, mdb, err)

	// Rollback anyway.
	if rollbackErr := xact.Rollback(); rollbackErr != nil {
		log.Fatal(rollbackErr)
	}
	err = nil

	// start another transaction
	xact, err = mdb.Begin()
	checkErr(t, mdb, err)

	// This should work fine.
	_, err = xact.Exec("INSERT user (first_name, last_name, age, username) VALUES (\"it's NOT CHABOY\", \"Smith\", 45, \"NOT CHABOY\")")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM user")
	checkErr(t, mdb, err)

	// Commit the transaction.
	err = xact.Commit()
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM user")
	checkErr(t, mdb, err)

	for rows.Next() {
		err = rows.Scan(
			&user.id,
			&user.firstName,
			&user.lastName,
			&user.age,
			&user.username,
		)
		checkErr(t, mdb, err)

		if user.username == "it's CHABOY" {
			mdb.Close()
			panic("rollback unsuccessful")
		}

		log.Printf("user: %v\n", user)
	}



	mdb.Close()
}

// Test Begin

// Test Close

func handleResult(t *testing.T, mdb *sql.DB, r sql.Result) (lastInsert int64, rowsAffected int64) {
	var err error
	lastInsert, err = r.LastInsertId()
	checkErr(t, mdb, err)

	rowsAffected, err = r.RowsAffected()
	checkErr(t, mdb, err)

	log.Printf("Last InsertID: %v, Rows Affected: %v ", lastInsert, rowsAffected)

	return
}

func checkErr(t *testing.T, mdb *sql.DB, err error) {
	if err != nil {
		mdb.Close()
		t.Error(err)
		panic("")
	}
}
