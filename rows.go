// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"io"
)

type Field struct {
	tableName string
	name      string
	flags     fieldFlag
	fieldType byte
	decimals  byte
}

type Rows interface {
	Columns() []string
	Close() error
	Next(dest ...interface{}) error
}

type iRows struct {
	conn    *Conn
	columns []Field
}

type binaryRows struct {
	iRows
}

type textRows struct {
	iRows
}

type emptyRows struct{}

func (rows *iRows) Columns() []string {
	columns := make([]string, len(rows.columns))
	if rows.conn.cfg.ColumnsWithAlias {
		for i := range columns {
			if tableName := rows.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.columns[i].name
			} else {
				columns[i] = rows.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.columns[i].name
		}
	}
	return columns
}

func (rows *iRows) Close() error {
	conn := rows.conn
	if conn == nil {
		return nil
	}
	if conn.netConn == nil {
		return ErrInvalidConn
	}

	// Remove unread packets from stream
	err := conn.readUntilEOF()
	rows.conn = nil
	return err
}

func (rows *binaryRows) Next(dest ...interface{}) error {
	if conn := rows.conn; conn != nil {
		if conn.netConn == nil {
			return ErrInvalidConn
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows *textRows) Next(dest ...interface{}) error {
	if conn := rows.conn; conn != nil {
		if conn.netConn == nil {
			return ErrInvalidConn
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows emptyRows) Columns() []string {
	return nil
}

func (rows emptyRows) Close() error {
	return nil
}

func (rows emptyRows) Next(dest ...interface{}) error {
	return io.EOF
}
