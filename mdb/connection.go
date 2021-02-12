// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type bsqlConn struct {
	buf              buffer
	netConn          net.Conn
	rawConn          net.Conn // underlying connection when netConn is TLS connection.
	affectedRows     uint64
	insertId         uint64
	cfg              *Config
	maxAllowedPacket int
	maxWriteSize     int
	writeTimeout     time.Duration
	flags            clientFlag
	status           statusFlag
	sequence         uint8
	parseTime        bool
	reset            bool // set when the Go SQL package calls ResetSession

	// for context support (Go 1.8+)
	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError // set non-nil if conn is canceled
	closed   atomicBool  // set when conn is closed, before closech is closed
}

// Handles parameters set in DSN after the connection is established
func (bc *bsqlConn) handleParams() (err error) {
	var cmdSet strings.Builder
	for param, val := range bc.cfg.Params {
		switch param {
		// Charset: character_set_connection, character_set_client, character_set_results
		case "charset":
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = bc.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// Other system vars accumulated in a single SET command
		default:
			if cmdSet.Len() == 0 {
				// Heuristic: 29 chars for each other key=value to reduce reallocations
				cmdSet.Grow(4 + len(param) + 1 + len(val) + 30*(len(bc.cfg.Params)-1))
				cmdSet.WriteString("SET ")
			} else {
				cmdSet.WriteByte(',')
			}
			cmdSet.WriteString(param)
			cmdSet.WriteByte('=')
			cmdSet.WriteString(val)
		}
	}

	if cmdSet.Len() > 0 {
		err = bc.exec(cmdSet.String())
		if err != nil {
			return
		}
	}

	return
}

func (bc *bsqlConn) markBadConn(err error) error {
	if bc == nil {
		return err
	}
	if err != errBadConnNoWrite {
		return err
	}
	return driver.ErrBadConn
}

func (bc *bsqlConn) Begin() (driver.Tx, error) {
	return bc.begin(false)
}

func (bc *bsqlConn) begin(readOnly bool) (driver.Tx, error) {
	if bc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	var q string
	if readOnly {
		q = "START TRANSACTION READ ONLY"
	} else {
		q = "START TRANSACTION"
	}
	err := bc.exec(q)
	if err == nil {
		return &bsqlTx{bc}, err
	}
	return nil, bc.markBadConn(err)
}

func (bc *bsqlConn) Close() (err error) {
	// Makes Close idempotent
	if !bc.closed.IsSet() {
		err = bc.writeCommandPacket(comQuit)
	}

	bc.cleanup()

	return
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because MySQL will have already
// closed the network connection.
func (bc *bsqlConn) cleanup() {
	if !bc.closed.TrySet(true) {
		return
	}

	// Makes cleanup idempotent
	close(bc.closech)
	if bc.netConn == nil {
		return
	}
	if err := bc.netConn.Close(); err != nil {
		errLog.Print(err)
	}
}

func (bc *bsqlConn) error() error {
	if bc.closed.IsSet() {
		if err := bc.canceled.Value(); err != nil {
			return err
		}
		return ErrInvalidConn
	}
	return nil
}

func (bc *bsqlConn) Prepare(query string) (driver.Stmt, error) {
	if bc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := bc.writeCommandPacketStr(comStmtPrepare, query)
	if err != nil {
		// STMT_PREPARE is safe to retry.  So we can return ErrBadConn here.
		errLog.Print(err)
		return nil, driver.ErrBadConn
	}

	stmt := &bsqlStmt{
		bc: bc,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = bc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = bc.readUntilEOF()
		}
	}

	return stmt, err
}

func (bc *bsqlConn) interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf, err := bc.buf.takeCompleteBuffer()
	if err != nil {
		// can not take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return "", ErrInvalidConn
	}
	buf = buf[:0]
	argPos := 0

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
				buf, err = appendDateTime(buf, v.In(bc.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			if bc.status&statusNoBackslashEscapes == 0 {
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
				if bc.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if bc.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > bc.maxAllowedPacket {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func (bc *bsqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if bc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !bc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := bc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	bc.affectedRows = 0
	bc.insertId = 0

	err := bc.exec(query)
	if err == nil {
		return &mysqlResult{
			affectedRows: int64(bc.affectedRows),
			insertId:     int64(bc.insertId),
		}, err
	}
	return nil, bc.markBadConn(err)
}

// Internal function to execute commands
func (bc *bsqlConn) exec(query string) error {
	// Send command
	if err := bc.writeCommandPacketStr(comQuery, query); err != nil {
		return bc.markBadConn(err)
	}

	// Read Result
	resLen, err := bc.readResultSetHeaderPacket()
	if err != nil {
		return err
	}

	if resLen > 0 {
		// columns
		if err := bc.readUntilEOF(); err != nil {
			return err
		}

		// rows
		if err := bc.readUntilEOF(); err != nil {
			return err
		}
	}

	return bc.discardResults()
}

func (bc *bsqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return bc.query(query, args)
}

func (bc *bsqlConn) query(query string, args []driver.Value) (*textRows, error) {
	if bc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !bc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := bc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	// Send command
	err := bc.writeCommandPacketStr(comQuery, query)
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

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (bc *bsqlConn) getSystemVar(name string) ([]byte, error) {
	// Send command
	if err := bc.writeCommandPacketStr(comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := bc.readResultSetHeaderPacket()
	if err == nil {
		rows := new(textRows)
		rows.bc = bc
		rows.rs.columns = []mysqlField{{fieldType: fieldTypeVarChar}}

		if resLen > 0 {
			// Columns
			if err := bc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		dest := make([]driver.Value, resLen)
		if err = rows.readRow(dest); err == nil {
			return dest[0].([]byte), bc.readUntilEOF()
		}
	}
	return nil, err
}

// finish is called when the query has canceled.
func (bc *bsqlConn) cancel(err error) {
	bc.canceled.Set(err)
	bc.cleanup()
}

// finish is called when the query has succeeded.
func (bc *bsqlConn) finish() {
	if !bc.watching || bc.finished == nil {
		return
	}
	select {
	case bc.finished <- struct{}{}:
		bc.watching = false
	case <-bc.closech:
	}
}

// Ping implements driver.Pinger interface
func (bc *bsqlConn) Ping(ctx context.Context) (err error) {
	if bc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err = bc.watchCancel(ctx); err != nil {
		return
	}
	defer bc.finish()

	if err = bc.writeCommandPacket(comPing); err != nil {
		return bc.markBadConn(err)
	}

	return bc.readResultOK()
}

// BeginTx implements driver.ConnBeginTx interface
func (bc *bsqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bc.closed.IsSet() {
		return nil, driver.ErrBadConn
	}

	if err := bc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer bc.finish()

	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		level, err := mapIsolationLevel(opts.Isolation)
		if err != nil {
			return nil, err
		}
		err = bc.exec("SET TRANSACTION ISOLATION LEVEL " + level)
		if err != nil {
			return nil, err
		}
	}

	return bc.begin(opts.ReadOnly)
}

func (bc *bsqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := bc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := bc.query(query, dargs)
	if err != nil {
		bc.finish()
		return nil, err
	}
	rows.finish = bc.finish
	return rows, err
}

func (bc *bsqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := bc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer bc.finish()

	return bc.Exec(query, dargs)
}

func (bc *bsqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := bc.watchCancel(ctx); err != nil {
		return nil, err
	}

	stmt, err := bc.Prepare(query)
	bc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *bsqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.bc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := stmt.query(dargs)
	if err != nil {
		stmt.bc.finish()
		return nil, err
	}
	rows.finish = stmt.bc.finish
	return rows, err
}

func (stmt *bsqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.bc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.bc.finish()

	return stmt.Exec(dargs)
}

func (bc *bsqlConn) watchCancel(ctx context.Context) error {
	if bc.watching {
		// Reach here if canceled,
		// so the connection is already invalid
		bc.cleanup()
		return nil
	}
	// When ctx is already cancelled, don't watch it.
	if err := ctx.Err(); err != nil {
		return err
	}
	// When ctx is not cancellable, don't watch it.
	if ctx.Done() == nil {
		return nil
	}
	// When watcher is not alive, can't watch it.
	if bc.watcher == nil {
		return nil
	}

	bc.watching = true
	bc.watcher <- ctx
	return nil
}

func (bc *bsqlConn) startWatcher() {
	watcher := make(chan context.Context, 1)
	bc.watcher = watcher
	finished := make(chan struct{})
	bc.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-bc.closech:
				return
			}

			select {
			case <-ctx.Done():
				bc.cancel(ctx.Err())
			case <-finished:
			case <-bc.closech:
				return
			}
		}
	}()
}

func (bc *bsqlConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

// ResetSession implements driver.SessionResetter.
// (From Go 1.10)
func (bc *bsqlConn) ResetSession(ctx context.Context) error {
	if bc.closed.IsSet() {
		return driver.ErrBadConn
	}
	bc.reset = true
	return nil
}
