package mdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
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

	connector, err = d.OpenConnector("system:biglove@tcp(0.0.0.0:4123)/main")
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

func TestSQLMaxRows(t *testing.T) {
	var (
		mdb *sql.DB
		err error
		maxRows = 100
		batchSize = 11000
		totalRows = 129
		rows *sql.Rows
	)


	mdb, err = sql.Open("mdb", fmt.Sprintf("system:biglove@tcp(0.0.0.0:8080)/master?maxRowCount=%d&fetchSize=%d", maxRows, batchSize))
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT *, sys_xact FROM syscolumns")
	checkErr(t, mdb, err)

	count := 0
	for rows.Next() {
		count ++
	}

	if maxRows > totalRows {
		if count != totalRows {
			panic(fmt.Sprintf("Didn't stop at max rows: %d, stopped at: %d", maxRows, count))
		}
	} else {
		if count != maxRows {
			panic(fmt.Sprintf("Didn't stop at max rows: %d, stopped at: %d", maxRows, count))
		}
	}


	mdb.Close()
}

func TestBasicSQLImplementation(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		command struct{
			id 		  string `db:"id"`
			status    bool
			startTime 	  time.Time
			endTime  		time.Time
			duration float64
			username string
			commandType uint8
			SQLtext string
			xactId    uint64
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")
	checkErr(t, mdb, err)


	// Fail a query.
	rows, err = mdb.Query("SELECT * FROM skrt_skrt")

	rows, err = mdb.Query("SELECT *, sys_xact FROM sys_sessions")
	checkErr(t, mdb, err)


	for rows.Next() {
		err = rows.Scan(
			&command.id,
			&command.status,
			&command.startTime,
			&command.endTime,
			&command.duration,
			&command.username,
			&command.commandType,
			&command.SQLtext,
			&command.xactId,
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

func TestBasicAmend(t *testing.T) {
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

	result, err = mdb.Exec("CREATE BLOCKCHAIN user SPARSE " +
		"(id uint64 PRIMARY KEY AUTO INCREMENT [1, 1], " +
		"stripe_token string size=14, " +
		"first_name string size=50 PACKED, " +
		"last_name string size=100 PACKED, " +
		"email string size=255 PACKED UNIQUE, " +
		"phone_number string size=20 PACKED, " +
		"verified_email bool default=false, " +
		"verified_phone bool default=false, " +
		"two_factor_secret BYTE ARRAY nullable size=20, " +
		"password_hash BYTE ARRAY size=32, " +
		"salt BYTE ARRAY size=32)")
	checkErr(t, mdb, err)

	handleResult(t, mdb, result)

	result, err = mdb.Exec("INSERT user (stripe_token, first_name, last_name, email, phone_number, " +
		" two_factor_secret, password_hash, salt)  VALUES (" +
		"\"hi\", " +
		"\"hi\", " +
		"\"hi\", " +
		"\"hi\", " +
		"\"hi\", " +
		"[100], " +
		"[100], " +
		"[100] " +
		")")
	checkErr(t, mdb, err)

	result, err = mdb.Exec("AMEND user (id, verified_phone)" +
		" VALUES (" +
		"1, " +
		"true " +
		")")
	checkErr(t, mdb, err)

	result, err = mdb.Exec("AMEND user (id, verified_email)" +
		" VALUES (" +
		"1, " +
		"true " +
		")")
	checkErr(t, mdb, err)

	mdb.Close()

	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")
	checkErr(t, mdb, err)

	_, err = mdb.Query("SELECT * ")
	checkErr(t, mdb, err)
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


func TestImportFile(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		company struct{
			symbol string
			name string
			sector string
			price float32
			price_earning float32
			dividend_yield float32
			earning_share float32
			book_value float32
			ft_week_low float32
			ft_week_high float32
			market_cap float64
			EBITDA float64
			sales float64
			price_book_value float32
			SEC_filings string
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("USE main")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM IMPORT = \"test_files/constituents-financials.csv\" (symbol string primary = true padded, name string padded, sector string padded, price float32, price_earning float32, dividend_yield float32, earning_share float32,  book_value float32, 52_week_low float32, 52_week_high float32, market_cap float64, EBITDA float64, sales float64, price_book_value float32, SEC_filings string padded)")
	checkErr(t, mdb, err)

	count := 1
	for rows.Next() {
		log.Println("RECORD: " + fmt.Sprintf("%d", count) )
		err = rows.Scan(
			&company.symbol,
			&company.name,
			&company.sector,
			&company.price,
			&company.price_earning,
			&company.dividend_yield,
			&company.earning_share,
			&company.book_value,
			&company.ft_week_low,
			&company.ft_week_high,
			&company.market_cap,
			&company.EBITDA,
			&company.sales,
			&company.price_book_value,
			&company.SEC_filings,
		)
		checkErr(t, mdb, err)
		count ++
		log.Printf("user: %v\n", company)
	}

	mdb.Close()
}

func TestDemoDBSetup(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		price struct{
			symbol string
			price float32
			dividend_yield float32
			price_earning float32
			earning_share float32
			book_value float32
			ft_week_low float32
			ft_week_high float32
		}

		join struct{
			s_symbol string
			c_symbol string
			c_name string
			c_sector string
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/main")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("CREATE BLOCKCHAIN temp TRADITIONAL (symbol string primary = true packed, name string packed, sector string packed, price float32, dividend_yield float32, price_earning float32, earning_share float32, book_value float32, 52_week_low float32, 52_week_high float32, market_cap float64, EBITDA float64, sales float64, price_book_value float32, SEC_filings string packed)")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("CREATE BLOCKCHAIN companies HISTORICAL PLUS (symbol string primary = true packed, name string packed unique nullable, sector string packed default = \"Undefined\")")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("CREATE BLOCKCHAIN pricing SPARSE (symbol string primary = true packed, price float32, dividend_yield float32, price_earning float32, earning_share float32, book_value float32, 52_week_low float32 CHECK [52_week_high > 52_week_low], 52_week_high float32)")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT INTO temp SELECT * FROM IMPORT = \"test_files/constituents-financials.csv\" (symbol string primary = true packed, name string packed, sector string packed, price float32, price_earning float32, dividend_yield float32, earning_share float32,  book_value float32, 52_week_low float32, 52_week_high float32, market_cap float64, EBITDA float64, sales float64, price_book_value float32, SEC_filings string packed)")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT INTO companies SELECT symbol, name, sector FROM temp")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT INTO pricing SELECT symbol, price, dividend_yield, price_earning, earning_share, book_value, 52_week_low, 52_week_high FROM temp")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM pricing")
	checkErr(t, mdb, err)

	count := 1
	for rows.Next() {
		log.Println("RECORD: " + fmt.Sprintf("%d", count) )
		err = rows.Scan(
			&price.symbol,
			&price.price,
			&price.dividend_yield,
			&price.price_earning,
			&price.earning_share,
			&price.book_value,
			&price.ft_week_low,
			&price.ft_week_high,
		)
		checkErr(t, mdb, err)
		count ++
		log.Printf("pricing row: %v\n", price)
	}

	rows, err = mdb.Query("SELECT * FROM (SELECT pricing.symbol FROM pricing WHERE symbol = \"AAPL\") AS s JOIN (SELECT * FROM companies) AS c ON c.symbol = s.symbol")
	checkErr(t, mdb, err)


	count = 1
	for rows.Next() {
		log.Println("RECORD: " + fmt.Sprintf("%d", count) )
		err = rows.Scan(
			&join.s_symbol,
			&join.c_symbol,
			&join.c_name,
			&join.c_sector,
		)
		checkErr(t, mdb, err)
		count ++
		log.Printf("joined row: %v\n", join)
	}

	mdb.Close()
}

func TestBackupRestore(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		rows *sql.Rows

		//price struct{
		//	symbol string
		//	price float32
		//	dividend_yield float32
		//	price_earning float32
		//	earning_share float32
		//	book_value float32
		//	ft_week_low float32
		//	ft_week_high float32
		//}

		company struct{
			symbol string
			name string
			sector string
		}
	)


	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/main")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("USE main")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("CREATE BLOCKCHAIN temp TRADITIONAL (symbol string primary = true packed, name string packed, sector string packed, price float32, dividend_yield float32, price_earning float32, earning_share float32, book_value float32, 52_week_low float32, 52_week_high float32, market_cap float64, EBITDA float64, sales float64, price_book_value float32, SEC_filings string packed)")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("CREATE BLOCKCHAIN companies HISTORICAL PLUS (symbol string primary = true packed, name string packed unique nullable, sector string packed default = \"Undefined\")")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT INTO temp SELECT * FROM IMPORT = \"test_files/constituents-financials.csv\" (symbol string primary = true packed, name string packed, sector string packed, price float32, price_earning float32, dividend_yield float32, earning_share float32,  book_value float32, 52_week_low float32, 52_week_high float32, market_cap float64, EBITDA float64, sales float64, price_book_value float32, SEC_filings string packed)")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT INTO companies SELECT symbol, name, sector FROM temp")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("BACKUP main.companies TO DISK \"companies_backup\"")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("INSERT companies VALUES (\"POLA\", \"Polar Power, Inc\", \"Industrial\")")
	checkErr(t, mdb, err)

	_, err = mdb.Exec("RESTORE main.companies FROM \"bsql_backups/companies_backup.zip\"")
	checkErr(t, mdb, err)

	rows, err = mdb.Query("SELECT * FROM companies")
	checkErr(t, mdb, err)

	count := 1
	for rows.Next() {
		log.Println("RECORD: " + fmt.Sprintf("%d", count))
		err = rows.Scan(
			&company.symbol,
			&company.name,
			&company.sector,
		)
		checkErr(t, mdb, err)
		count ++
		log.Printf("company row: %v\n", company)
	}

	mdb.Close()
}


func TestSystemMetadata(t *testing.T) {
	var (
		mdb *sql.DB
		err error

		sysDB_rows,
		sysBC_rows *sql.Rows

		db struct{
			name 		  string
		}

		blockchain struct{
			bcName string
			bcId uint16
			blockchainType uint16
			cName string
			cOrder uint16
			cType uint8
		}
	)

	mdb, err = sql.Open("mdb", "system:biglove@tcp(0.0.0.0:8080)/master")

	// Fail a query.
	sysDB_rows, err = mdb.Query("SELECT name FROM sys_databases")
	for sysDB_rows.Next() {
		err = sysDB_rows.Scan(
			&db.name,
		)

		log.Printf("DB Name: %v\n", db)

		// Enter the first database.
		_, err = mdb.Exec(fmt.Sprintf("USE %s", db.name))
		checkErr(t, mdb, err)

		// Get the blockchains and their columns.
		sysBC_rows, err = mdb.Query(
			"SELECT  b.name, b.id, b.blockchain_type, c.name, c.order, c.type  " +
			"FROM sysblockchains AS b JOIN syscolumns AS c" +
			" ON b.id = c.blockchain_id WHERE b.id > 16 ")
		checkErr(t, mdb, err)

		for sysBC_rows.Next() {
			err = sysBC_rows.Scan(
				&blockchain.bcName,
				&blockchain.bcId,
				&blockchain.blockchainType,
				&blockchain.cName,
				&blockchain.cOrder,
				&blockchain.cType,
			)
			checkErr(t, mdb, err)
			log.Printf("BC: %v\n", blockchain)
		}
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
