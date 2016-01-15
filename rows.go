// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"fmt"
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
	Close() error
	Columns() []string
	Next() bool
	Scan(dest ...interface{}) error
}

type iRows struct {
	conn    *Conn
	columns []Field
	data    []byte
	err     error
}

type binaryRows struct {
	iRows
	nullMask []byte
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

func (rows *binaryRows) Next() bool {
	if conn := rows.conn; conn != nil {
		if conn.netConn == nil {
			rows.err = ErrInvalidConn
			return false
		}
		// Fetch next row from stream
		rows.err = rows.readRow()
		return rows.err != io.EOF
	}
	return false
}

func (rows *binaryRows) Scan(dest ...interface{}) (err error) {
	if err = rows.err; err != nil {
		return
	}
	if rows.data == nil {
		return ErrNoRow
	}
	if len(dest) != len(rows.columns) {
		fmt.Errorf("expected %d destination arguments in Scan, not %d", len(rows.columns), len(dest))
	}

	err = rows.convert(dest)
	rows.data = nil
	return
}

func (rows *textRows) Next() bool {
	if conn := rows.conn; conn != nil {
		if conn.netConn == nil {
			rows.err = ErrInvalidConn
			return false
		}
		// Fetch next row from stream
		rows.err = rows.readRow()
		return rows.err != io.EOF
	}
	return false
}

func (rows *textRows) Scan(dest ...interface{}) (err error) {
	if err = rows.err; err != nil {
		return
	}
	if rows.data == nil {
		return ErrNoRow
	}
	if len(dest) != len(rows.columns) {
		fmt.Errorf("expected %d destination arguments in Scan, not %d", len(rows.columns), len(dest))
	}

	err = rows.convert(dest)
	rows.data = nil
	return
}

func (rows emptyRows) Columns() []string {
	return nil
}

func (rows emptyRows) Close() error {
	return nil
}

func (rows emptyRows) Next() bool {
	return false
}

func (rows emptyRows) Scan(dest ...interface{}) error {
	return ErrNoRow
}
