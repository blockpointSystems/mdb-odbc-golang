# Connecting to an MDB database with Go's database/sql package

Table of Contents
- Requirements
- Installation
- Documentation
- Example

In this article we are going to explore using bSQL with Go. bSQL is a blockchain SQL that leverages the power of relational
data and blockchain security protocols. If you need to brush up on bSQL full documentation is available on the [bSQL website](https://bsql.org/).

### Documentation

For support, please refer to [StackOverflow](https://stackoverflow.com/questions/tagged/mdb-bp).
For documentation of the database/sql package refer to the [Go docs](https://golang.org/pkg/database/sql/).

### Requirements
- If you haven't set up an MDB instance you can set one up on the [blockpoint website](https://blockpointdb.com/). You will need 
  information from your instance and account to connect via Go.
- You will need an up-to-date version of Go as well.

### Installation

Install with go get:

```
$ go get -u github.com/blockpointSystems/mdb-odbc-golang
```

Before we proceed let's look at the information we need to collect.

````go
const (
	host = "0.0.0.0"
	port = "5461"
	user = "system"
	password = "<system-password>"
	dbname = "main"
)
````

Most of these are self-explanatory. 
- **connectionType**: should always specify a "tcp" connection.
- **host**: the IP Address of your MDB instance.
- **port**: port "5461" id the default port used for interfacing with MDB.
- **user**/**password**: your MDB credentials. It is important you use your MBD credentials,
  NOT the credentials associated with your blockpoint portal.
- **dbname**: the name of the database we are connecting to.


### Connecting to the database.

We can use the *bsqlInfo* to connect to the MDB using the golang database package. When *Open* is called, we establish a connection the MDB instance and new session
is created. The code below connects to the "main" database and closes the connection.

````go
package main

import (
	"database/sql"
	"fmt"
	"log"
	
	"github.com/blockpointSystems/mdb-odbc-golang"
)

var (
	mdb *sql.DB
    	err error
)

const (
	host = "0.0.0.0"
	port = "5461"
	user = "system"
	password = "<system-password>"
	dbname = "main"
)

func main() {
	// The connection string is a compilation of the constants we defined above.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)
	
	// We call sql.Open(...) to establish the database connection.
	mdb, err = sql.Open("mdb", dsn)
	if err != nil {
		panic(err)
	}
	defer mdb.Close()
}
````

### Executing Database Commands.

In order to execute database commands we can use the *Exec* function. We call *Exec* for all database commands that
don't return rows. This includes database mutations, management, security and most analytical commands. For documentation
on bSQL commands look at the [documentation](https://bsql.org/docs/management/create-blockchain). When *Exec* is called on
a database connection, a new transaction is created an executed atomically.

```go
// The connection string is a compilation of the constants we defined above.
dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

// We call sql.Open(...) to establish the database connection.
mdb, err = sql.Open("mdb", dsn)
if err != nil {
	panic(err)
}
defer mdb.Close()

// Create a new blockchain called "good_boys" of TRADITIONAL type.
_, err = mdb.Exec(`CREATE BLOCKCHAIN good_boys TRADITIONAL (id UINT8 PRIMARY, name STRING PACKED, email STRING PACKED NULLABLE)`)
if err != nil {
	panic(err)
}
// Insert two value atomically.
_, err = mdb.Exec(`INSERT good_boys VALUES (1, \"rolly\", \"rolly@corgicutie.com\"), (2, \"hank\", \"hank@neigbordog.com\")`)
if err != nil {
	panic(err)
}
```


### Executing Database Commands Using Transaction.

We can use transactions to execute mutations non-atomically. The default isolation level in bSQL is *read uncommitted*, 
you can read more about isolation levels and transaction atomicity on the bSQL site. Using transactions gives us more 
control of database writes using the *Rollback()* and *Commit()* methods.


```go
// The connection string is a compilation of the constants we defined above.
dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

// We call sql.Open(...) to establish the database connection.
mdb, err = sql.Open("mdb", dsn)
if err != nil {
	panic(err)
}
defer mdb.Close()

// Begin a transaction.
xact, err = mdb.Begin()
if err != nil {
	panic(err)
}

// Creating a blockchain using a transaction STILL executes atomically.
_, err = xact.Exec("CREATE BLOCKCHAIN users TRADITIONAL (id UINT8 PRIMARY, name STRING PACKED, email STRING PACKED NULLABLE)")
if err != nil {
	panic(err)
}

// Insert two value non- atomically.
_, err = mdb.Exec("INSERT users VALUES (1, \"rolly\", \"rolly@corgicutie.com\"), (2, \"hank\", \"hank@neigbordog.com\")")
if err != nil {
	// We can rollback in the case of an insertion error, undoing the above mutation.
	if rollbackErr := xact.Rollback(); rollbackErr != nil {
		panic(rollbackErr)
	}
	panic(err)
}

// Commit the transaction
err = xact.Commit()
if err != nil {
	panic(err)
}
```

### Executing Queries.

This example queries from the *sys_sessions* system blockchain. A few things to note:
- In this example we connect to the master database by specifying "master" in our connection string.
- *Query()* returns a **sql.Rows* object that can be iterated through.
- Until the *rows* are closed or iterated until the end, the query remains open and additional commands will fail.


```go
// The connection string is a compilation of the constants we defined above.
dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

// We call sql.Open(...) to establish the database connection.
mdb, err = sql.Open("mdb", dsn)
if err != nil {
	panic(err)
}
defer mdb.Close()

// Define our rows and corresponding object variable.
var (
	rows *sql.Rows

	command struct{
		id		string 
		status       	bool
		startTime 	time.Time
		endTime  	time.Time
		duration        float64
		username        string
		commandType     uint8
		SQLtext         string
		xactId          uint64
	}
)

// Query from the sys_session system blockchain, specifying the transaction ID as well.
rows, err = mdb.Query("SELECT *, sys_xact FROM sys_sessions")
if err != nil {
	panic(err)
}

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
	if err != nil {
		panic(err)
	}
	log.Printf("command: %v\n", command)
}
```

## Resources 
- bSQL documentation: https://bsql.org/
- Go docs https://golang.org/pkg/database/sql/
