package mdb

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gitlab.com/blockpoint/mdb-odbc-golang/protocolBuffers/v1/odbc"
	"math"
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
	cfg    *Config
	status statusFlag

	// TODO: Make atomic
	closed bool

	// Operational
	odbc.MDBServiceClient
	auth *odbc.AuthPacket

	// Query
	activeQuery         bool
	queryResponseStream *odbc.MDBService_QueryClient
}

// Handles parameters set in DSN after the connection is established
func (db *Conn) configureConnection() (err error) {
	// Nothing to really do here, only the basic charset is allowed

	// Resolve the auth packet
	if db.auth == nil {
		db.auth = new(odbc.AuthPacket)
	}

	var (
		initReq  = &odbc.InitializationRequest{
			Username: db.cfg.User,
			Password: db.cfg.Password,
			DbName:   db.cfg.DBName,
			Auth: 	  db.auth,
		}
	)

	db.auth, err = db.MDBServiceClient.InitializeConnection(
		context.Background(),
		initReq,
	)


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
	s = &Stmt{
		conn:       db,
		stmt:       query,
		paramCount: strings.Count(query, "?"),
	}
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
	if !db.IsClosed() {
		db.SetClosed()
		_, err = db.MDBServiceClient.Close(context.Background(), db.auth)
		db.MDBServiceClient = nil
	}
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
			Auth: db.auth,
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
			Auth: 	   db.auth,
			Statement: query,
		}

		resp *odbc.ExecResponse
	)

	// Send the command
	resp, err = db.MDBServiceClient.Exec(context.Background(), req)
	if err != nil {
		return
	}
	// TODO: Update JWT

	// Log affected Rows
	affectedRows = resp.AffectedRows
	// Log insert Id
	insertId     = resp.InsertId

	return
}

func (db *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return db.query(query, args)
}

func (db *Conn) query(query string, args []driver.Value) (*Rows, error) {
	var (
		req        *odbc.QueryRequest
		respClient odbc.MDBService_QueryClient

		queryResp *odbc.QueryResponse

		err  error
	)

	if db.IsClosed() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	// Lock, check activeQuery flag, if not active, update, and unlock; set activeQuery to true.
	if db.activeQuery {
		return &Rows{}, fmt.Errorf("query already active")
	}
	db.activeQuery = true


	if len(args) != 0 {
		if !db.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}

		query, err = db.interpolateParams(query, args)
		if err != nil {
			db.activeQuery = false
			return nil, db.markBadConn(err)
		}
	}

	req = &odbc.QueryRequest{
		Auth: 	   db.auth,
		Statement: query,
	}

	// Send command
	respClient, err = db.MDBServiceClient.Query(context.Background(), req)
	if err != nil {
		db.activeQuery = false
		return nil, db.markBadConn(err)
	}


	// Store the stream in the connection object
	//Now stored in the rows directly
	db.queryResponseStream = &respClient

	// Grab the first result set
	queryResp, err = respClient.Recv()
	if err != nil {
		db.activeQuery = false
		return &Rows{}, err
	}

	// Deserialize the response and build the rows
	var resp = &Rows{
		streamRecv: &respClient,
		schema: queryResp.GetRespSchema(),
		set:  buildResultSet(queryResp.GetRespSchema(), queryResp.GetResultSet()),
		done: queryResp.GetDone(),
	}
	//if resp.done {
	//	// Close the query req and return the rows
	//	respClient.CloseSend()
	//
	//	// FIXME: Update this
	//	db.activeQuery = false
	//	resp.close = func() error { return nil }
	//	return resp, nil
	//}

	resp.close = func() error {
		if !db.activeQuery {
			return fmt.Errorf("query active but hasn't been closed")
		}

		db.activeQuery = false
		return (*db.queryResponseStream).CloseSend()
	}

	return resp, nil
}

func buildResultSet(schema *odbc.Schema, set []*odbc.Row) (rs resultSet) {
	if schema.GetTableName() == "" {
		rs.columnNames = schema.GetColumnName()
	} else {
		rs.columnNames = make([]string, len(schema.GetColumnName()))
		for i, colName := range schema.GetColumnName() {
			rs.columnNames[i] = fmt.Sprintf("%s.%s", schema.GetTableName(), colName)
		}
	}

	rs.buildNextResultSet(schema, set)
	return
}

func convertColumnToValue(col []byte, datatype odbc.Datatype) driver.Value {
	// TODO: Test the int / uint cases

	//if len(col) == 0 {
	//	//panic("column data null")
	//	return nil
	//}

	switch datatype {
	case odbc.Datatype_BYTEARRAY:
		return col
	case odbc.Datatype_STRING:
		return string(col)
	case odbc.Datatype_INT8:
		return int64(int8(col[0]))
	case odbc.Datatype_UINT8:
		return int64(col[0])
	case odbc.Datatype_INT16:
		return int64(int16(binary.LittleEndian.Uint16(col)))
	case odbc.Datatype_UINT16:
		return int64(binary.LittleEndian.Uint16(col))
	case odbc.Datatype_INT32:
		return int64(int32(binary.LittleEndian.Uint32(col)))
	case odbc.Datatype_UINT32:
		return int64(binary.LittleEndian.Uint32(col))
	case odbc.Datatype_INT64:
		return int64(binary.LittleEndian.Uint64(col))
	case odbc.Datatype_UINT64:
		return int64(binary.LittleEndian.Uint64(col))
	case odbc.Datatype_FLOAT32:
		bits := binary.LittleEndian.Uint32(col)
		return float64(math.Float32frombits(bits))
	case odbc.Datatype_FLOAT64:
		bits := binary.LittleEndian.Uint64(col)
		return math.Float64frombits(bits)
	case odbc.Datatype_BOOL:
		return bool(int8(col[0]) == 1)
	case odbc.Datatype_TIMESTAMP:
		return time.Unix(0, int64(binary.LittleEndian.Uint64(col[0:8])))
	case odbc.Datatype_UUID:
		uuid, _ := uuid.FromBytes(col)
		return uuid.String()
	}
	return nil
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
				buf = append(buf, '"')
				buf, err = appendDateTime(buf, v.In(db.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '"')
			}
		case json.RawMessage:
			buf = append(buf, '"')
			if db.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '"')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				//buf = append(buf, "_binary'"...)
				//if db.status&statusNoBackslashEscapes == 0 {
				//	buf = escapeBytesBackslash(buf, v)
				//} else {
				//	buf = escapeBytesQuotes(buf, v)
				//}
				//buf = append(buf, '"')
				buf = append(
					buf,
					fmt.Sprintf("%d", v)...,
				)
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
