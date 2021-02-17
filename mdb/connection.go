package mdb

import (
	"database/sql/driver"
	"encoding/json"
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
	cfg    Config
	status statusFlag

	// Operational

}

// Prepare returns a prepared statement, bound to this connection.
func (db *Conn)	Prepare(query string) (s Stmt, err error) {
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
func (db *Conn)	Begin() (xact Tx, err error) {
	panic("implement me")
	return
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

func (db *Conn) Exec(query string, args []driver.Value) (result Result, err error) {
	// Make sure connection is still live
	if db.IsClosed() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	// Interpolate parameters if provided
	if len(args) != 0 {
		if !db.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := db.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}

	// Reset affected rows
	db.affectedRows = 0
	db.insertId = 0

	err = db.exec(query)
	if err == nil {
		result = Result{
			affectedRows: int64(db.affectedRows),
			insertId:     int64(db.insertId),
		}
	}
	return
}

// Internal function to execute commands
func (db *Conn) exec(query string) (err error) {
	// Send command

	// Read Result

	// Log affected Rows
	// Log insert Id

	// Clean up

	// Return
	return
}

//func (bc *bsqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
//	return bc.query(query, args)
//}
//
//func (bc *bsqlConn) query(query string, args []driver.Value) (*textRows, error) {
//	if bc.closed.IsSet() {
//		errLog.Print(ErrInvalidConn)
//		return nil, driver.ErrBadConn
//	}
//	if len(args) != 0 {
//		if !bc.cfg.InterpolateParams {
//			return nil, driver.ErrSkip
//		}
//		// try client-side prepare to reduce roundtrip
//		prepared, err := bc.interpolateParams(query, args)
//		if err != nil {
//			return nil, err
//		}
//		query = prepared
//	}
//	// Send command
//	err := bc.writeCommandPacketStr(comQuery, query)
//	if err == nil {
//		// Read Result
//		var resLen int
//		resLen, err = bc.readResultSetHeaderPacket()
//		if err == nil {
//			rows := new(textRows)
//			rows.bc = bc
//
//			if resLen == 0 {
//				rows.rs.done = true
//
//				switch err := rows.NextResultSet(); err {
//				case nil, io.EOF:
//					return rows, nil
//				default:
//					return nil, err
//				}
//			}
//
//			// Columns
//			rows.rs.columns, err = bc.readColumns(resLen)
//			return rows, err
//		}
//	}
//	return nil, bc.markBadConn(err)
//}

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
