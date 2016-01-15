// gmysql - A MySQL package for Go
//
// Copyright 2016 The gmysql Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"fmt"
	"io"
)

// Field contains meta-data for one field
type Field struct {
	tableName string
	name      string
	flags     fieldFlag
	fieldType byte
	decimals  byte
}

// Rows is the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows:
//
//     rows, err := conn.Query("SELECT ...")
//     ...
//     defer rows.Close()
//     for rows.Next() {
//         var id int
//         var name string
//         err = rows.Scan(&id, &name)
//         ...
//     }
//     err = rows.Err() // get any error encountered during iteration
//     ...
type Rows interface {
	// Close closes the Rows, preventing further enumeration. If Next returns
	// false, the Rows are closed automatically and it will suffice to check the
	// result of Err. Close is idempotent and does not affect the result of Err.
	Close() error

	// Columns returns the column names.
	Columns() []string

	// Next prepares the next result row for reading with the Scan method.  It
	// returns true on success, or false if there is no next result row or an
	// error happened while preparing it. Err should be consulted to distinguish
	// between the two cases.
	//
	// Every call to Scan, even the first one, must be preceded by a call to
	// Next.
	Next() bool

	// Scan copies the columns in the current row into the values pointed
	// at by dest.
	//
	// If an argument has type *[]byte, Scan saves in that argument a copy
	// of the corresponding data. The copy is owned by the caller and can
	// be modified and held indefinitely. The copy can be avoided by using
	// an argument of type *RawBytes instead; see the documentation for
	// RawBytes for restrictions on its use.
	//
	// If an argument has type *interface{}, Scan copies the value
	// provided without conversion. If the value is of type []byte, a copy is
	// made and the caller owns the result.
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
		return ErrNoRows
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
		return ErrNoRows
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
	return ErrNoRows
}
