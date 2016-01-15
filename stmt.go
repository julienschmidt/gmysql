// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authorstmt. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

type Stmt struct {
	conn       *Conn
	id         uint32
	paramCount int
	columns    []Field // cached from the first query
}

func (conn *Conn) Prepare(query string) (*Stmt, error) {
	if conn.netConn == nil {
		return nil, ErrInvalidConn
	}
	// Send command
	err := conn.writeCommandPacketStr(comStmtPrepare, query)
	if err != nil {
		return nil, err
	}

	stmt := &Stmt{
		conn: conn,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = conn.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = conn.readUntilEOF()
		}
	}

	return stmt, err
}

func (stmt *Stmt) Close() error {
	if stmt.conn == nil || stmt.conn.netConn == nil {
		return ErrInvalidConn
	}

	err := stmt.conn.writeCommandPacketUint32(comStmtClose, stmt.id)
	stmt.conn = nil
	return err
}

func (stmt *Stmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *Stmt) Exec(args ...interface{}) (*Result, error) {
	if stmt.conn.netConn == nil {
		return nil, ErrInvalidConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	conn := stmt.conn
	conn.affectedRows = 0
	conn.insertId = 0

	// Read Result
	resLen, err := conn.readResultSetHeaderPacket()
	if err == nil {
		if resLen > 0 {
			// Columns
			err = conn.readUntilEOF()
			if err != nil {
				return nil, err
			}

			// Rows
			err = conn.readUntilEOF()
		}
		if err == nil {
			return &Result{
				affectedRows: int64(conn.affectedRows),
				insertId:     int64(conn.insertId),
			}, nil
		}
	}

	return nil, err
}

func (stmt *Stmt) Query(args ...interface{}) (Rows, error) {
	if stmt.conn.netConn == nil {
		return nil, ErrInvalidConn
	}
	// Send command
	err := stmt.writeExecutePacket(args)
	if err != nil {
		return nil, err
	}

	conn := stmt.conn

	// Read Result
	resLen, err := conn.readResultSetHeaderPacket()
	if err != nil {
		return nil, err
	}

	br := new(binaryRows)
	br.conn = conn

	if resLen > 0 {
		// Columns
		// If not cached, read them and cache them
		if stmt.columns == nil {
			stmt.columns, err = conn.readColumns(resLen)
		} else {
			err = conn.readUntilEOF()
		}
		br.columns = stmt.columns
	}

	return br, err
}
