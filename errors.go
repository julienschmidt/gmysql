// gmysql - A MySQL package for Go
//
// Copyright 2016 The gmysql Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"errors"
	"fmt"
	"log"
	"os"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn       = errors.New("invalid Connection")
	ErrMalformPkt        = errors.New("malformed Packet")
	ErrNoTLS             = errors.New("TLS encryption requested but server does not support TLS")
	ErrOldPassword       = errors.New("this user requires old password authentication. If you still want to use it, please add 'allowOldPasswords=1' to your DSN. See also https://github.com/go-sql-driver/mysql/wiki/old_passwords")
	ErrCleartextPassword = errors.New("this user requires clear text authentication. If you still want to use it, please add 'allowCleartextPasswords=1' to your DSN")
	ErrUnknownPlugin     = errors.New("the authentication plugin is not supported")
	ErrOldProtocol       = errors.New("MySQL-Server does not support required Protocol 41+")
	ErrPktSync           = errors.New("commands out of sync. You can't run this command now")
	ErrPktSyncMul        = errors.New("commands out of sync. Did you run multiple statements at once?")
	ErrPktTooLarge       = errors.New("packet for query is too large. You can change this value on the server by adjusting the 'max_allowed_packet' variable")
	ErrBusyBuffer        = errors.New("busy buffer")
	ErrUnsafeInterpolate = errors.New("this type can not safely be interpolated. Use prepared statements instead or build the query manually")
	ErrInterpolateFailed = errors.New("interpolating query failed")
	ErrNoRow             = errors.New("no row available")
)

var errLog = log.New(os.Stderr, "[MySQL] ", log.Ldate|log.Ltime|log.Lshortfile)

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	errLog = logger
	return nil
}

// Error is an error type which represents a single MySQL error
type Error struct {
	Number  uint16
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("Error %d: %s", e.Number, e.Message)
}

// Warnings is an error type which represents a group of one or more MySQL
// warnings
type Warnings []Warning

func (ws Warnings) Error() string {
	var msg string
	for i, warning := range ws {
		if i > 0 {
			msg += "\r\n"
		}
		msg += fmt.Sprintf(
			"%s %s: %s",
			warning.Level,
			warning.Code,
			warning.Message,
		)
	}
	return msg
}

// Warning is an error type which represents a single MySQL warning.
// Warnings are returned in groups only. See MySQLWarnings
type Warning struct {
	Level   string
	Code    string
	Message string
}

func (conn *Conn) getWarnings() (err error) {
	rows, err := conn.Query("SHOW WARNINGS", nil)
	if err != nil {
		return
	}

	var warnings = Warnings{}
	var values = make([]interface{}, 3)

	for rows.Next() {
		if err = rows.Scan(values); err != nil {
			rows.Close()
			return
		}

		warning := Warning{}

		if raw, ok := values[0].([]byte); ok {
			warning.Level = string(raw)
		} else {
			warning.Level = fmt.Sprintf("%s", values[0])
		}
		if raw, ok := values[1].([]byte); ok {
			warning.Code = string(raw)
		} else {
			warning.Code = fmt.Sprintf("%s", values[1])
		}
		if raw, ok := values[2].([]byte); ok {
			warning.Message = string(raw)
		} else {
			warning.Message = fmt.Sprintf("%s", values[0])
		}

		warnings = append(warnings, warning)
	}
	return warnings
}
