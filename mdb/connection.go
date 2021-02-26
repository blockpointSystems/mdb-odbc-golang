package mdb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"gitlab.com/blockpoint/utilities/odbc/mdb/protocolBuffers/odbc"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

// Conn is a connection to a database. It is not used concurrently
// by multiple goroutines.
//
// Conn is assumed to be stateful.
type Conn struct {
	// Managerial
	cfg   *Config
	status statusFlag

	// Operational
	odbc.MDBServiceClient

	// Query
	activeQuery bool
	queryResponseStream odbc.MDBService_QueryClient
}

// Handles parameters set in DSN after the connection is established
func (db *Conn) configureConnection() (err error) {
	// Nothing to really do here, only the basic charset is allowed

	//var cmdSet strings.Builder
	//for param, val := range db.cfg.Params {
	//	switch param {
	//	// Charset: character_set_connection, character_set_client, character_set_results
	//	case "charset":
	//		charsets := strings.Split(val, ",")
	//		for i := range charsets {
	//			// ignore errors here - a charset may not exist
	//			_, _, err = db.exec("SET NAMES " + charsets[i])
	//			if err == nil {
	//				break
	//			}
	//		}
	//		if err != nil {
	//			return
	//		}
	//
	//	// Other system vars accumulated in a single SET command
	//	default:
	//		if cmdSet.Len() == 0 {
	//			// Heuristic: 29 chars for each other key=value to reduce reallocations
	//			cmdSet.Grow(4 + len(param) + 1 + len(val) + 30*(len(db.cfg.Params)-1))
	//			cmdSet.WriteString("SET ")
	//		} else {
	//			cmdSet.WriteByte(',')
	//		}
	//		cmdSet.WriteString(param)
	//		cmdSet.WriteByte('=')
	//		cmdSet.WriteString(val)
	//	}
	//}
	//
	//if cmdSet.Len() > 0 {
	//	err = db.exec(cmdSet.String())
	//	if err != nil {
	//		return
	//	}
	//}

	return
}

// Prepare returns a prepared statement, bound to this connection.
func (db *Conn)	Prepare(query string) (s driver.Stmt, err error) {
	panic("implement me")
	return
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (db *Conn)	Close() (err error) {
	panic("implement me")
	return
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (db *Conn)	Begin() (xact driver.Tx, err error) {
	return db.begin(context.Background(), DEFAULT_XACT_OPTIONS)
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (db *Conn) BeginTx(ctx context.Context, xactOpts driver.TxOptions) (driver.Tx, error) {
	return db.begin(ctx, xactOpts)
}

func (db *Conn) begin(ctx context.Context, xactOpts driver.TxOptions) (xact driver.Tx, err error) {
	if db.IsClosed() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	var (
		req = &odbc.XactRequest{
			IsolationLevel: int32(xactOpts.Isolation),
			ReadOnly:       xactOpts.ReadOnly,
		}
		resp *odbc.XactResponse
	)
	resp, err = db.MDBServiceClient.Begin(ctx, req)
	if err != nil {
		errLog.Print(err)
		//err = driver.ErrBadConn
		err = db.markBadConn(err)
		return
	}

	return CreateTransaction(resp.GetXactId(), DEFAULT_XACT_OPTIONS, db), err
}

// Execer is an optional interface that may be implemented by a Conn.
//
// If a Conn implements neither ExecerContext nor Execer,
// the sql package's DB.Exec will first prepare a query, execute the statement,
// and then close the statement.
//
// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead.
//type Execer interface {}
func (db *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	var (
		result Result
		err    error
	)

	// Make sure connection is still live
	if db.IsClosed() {
		errLog.Print(ErrInvalidConn)
		err = driver.ErrBadConn
		return nil, err
	}

	// Interpolate parameters if provided
	if len(args) != 0 {
		if !db.cfg.InterpolateParams {
			err = driver.ErrBadConn
			return nil, err
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := db.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	result.affectedRows, result.insertId, err = db.exec(query)
	return &result, err
}

// Internal function to execute commands
func (db *Conn) exec(query string) (affectedRows, insertId int64, err error) {
	var (
		req = &odbc.ExecRequest{
			Statement: query,
		}

		resp *odbc.ExecResponse
	)

	// Send the command
	resp, err = db.MDBServiceClient.Exec(context.Background(), req)
	if err != nil {
		return
	}

	// Log affected Rows
	affectedRows = resp.AffectedRows
	// Log insert Id
	insertId     = resp.InsertId

	return
}

func (bc *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return bc.query(query, args)
}

func (bc *Conn) query(query string, args []driver.Value) (*textRows, error) {
	var (
		req  	  *odbc.QueryRequest
		respClient odbc.MDBService_QueryClient
		err  error
	)

	if bc.IsClosed() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	// Lock, check activeQuery flag, if not active, update, and unlock; set activeQuery to true.


	if len(args) != 0 {
		if !bc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}

		query, err = bc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
	}

	req = &odbc.QueryRequest{
		Statement: query,
	}

	// Send command
	respClient, err = bc.MDBServiceClient.Query(context.Background(), req)
	if err != nil {
		return nil, err
	}


	// Store the stream in the connection object
	bc.queryResponseStream = respClient

	// Grab the first result set
	resp, err = bc.queryResponseStream.Recv()
	if err == io.EOF {
		// No results exist. Close the stream and the query.
	}

	done := make(chan bool)

	go func() {
		for {
			resp, err := respClient.Recv()
			if err == io.EOF {
				done <- true
				return
			}
			if err != nil {
				log.Printf("Cannont receive %v", err)
			}

		}
	}()

	<- done


	err = bc.writeCommandPacketStr(comQuery, query)
	if err == nil {
		// Read Result
		var resLen int
		resLen, err = bc.readResultSetHeaderPacket()
		if err == nil {
			rows := new(textRows)
			rows.bc = bc

			if resLen == 0 {
				rows.rs.done = true

				switch err := rows.NextResultSet(); err {
				case nil, io.EOF:
					return rows, nil
				default:
					return nil, err
				}
			}

			// Columns
			rows.rs.columns, err = bc.readColumns(resLen)
			return rows, err
		}
	}
	return nil, bc.markBadConn(err)
}

func (db *Conn) interpolateParams(query string, args []driver.Value) (resp string, err error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		err = driver.ErrSkip
		return
	}


	// Initialize the buffer
	var (
		buf = make([]byte, 0, len(query))
		argPos int
	)

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf, err = appendDateTime(buf, v.In(db.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			if db.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if db.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '"')
			if db.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '"')
		default:
			return "", driver.ErrSkip
		}

		if len(buf) > db.GetMaxPacketSize() {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}
