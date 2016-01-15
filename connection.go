// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"net"
	"strconv"
	"strings"
	"time"
)

type Conn struct {
	buf              buffer
	netConn          net.Conn
	affectedRows     uint64
	insertID         uint64
	cfg              *Config
	maxPacketAllowed int
	maxWriteSize     int
	flags            clientFlag
	status           statusFlag
	sequence         uint8
	parseTime        bool
	strict           bool
}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
type DialFunc func(addr string) (net.Conn, error)

var dials map[string]DialFunc

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
func RegisterDial(net string, dial DialFunc) {
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

// Open opens a new connection
func Open(dsn string) (*Conn, error) {
	var err error

	// New mysqlConn
	conn := &Conn{
		maxPacketAllowed: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
	}
	conn.cfg, err = ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn.parseTime = conn.cfg.ParseTime
	conn.strict = conn.cfg.Strict

	// Connect to Server
	if dial, ok := dials[conn.cfg.Net]; ok {
		conn.netConn, err = dial(conn.cfg.Addr)
	} else {
		nd := net.Dialer{Timeout: conn.cfg.Timeout}
		conn.netConn, err = nd.Dial(conn.cfg.Net, conn.cfg.Addr)
	}
	if err != nil {
		return nil, err
	}

	// Enable TCP Keepalives on TCP connections
	if tc, ok := conn.netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			// Don't send COM_QUIT before handshake.
			conn.netConn.Close()
			conn.netConn = nil
			return nil, err
		}
	}

	conn.buf = newBuffer(conn.netConn)

	// Reading Handshake Initialization Packet
	cipher, err := conn.readInitPacket()
	if err != nil {
		conn.cleanup()
		return nil, err
	}

	// Send Client Authentication Packet
	if err = conn.writeAuthPacket(cipher); err != nil {
		conn.cleanup()
		return nil, err
	}

	// Handle response to auth packet, switch methods if possible
	if err = conn.handleAuthResult(cipher); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		conn.cleanup()
		return nil, err
	}

	// Get max allowed packet size
	maxap, err := conn.getSystemVar("max_allowed_packet")
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.maxPacketAllowed = stringToInt(maxap) - 1
	if conn.maxPacketAllowed < maxPacketSize {
		conn.maxWriteSize = conn.maxPacketAllowed
	}

	// Handle DSN Params
	err = conn.handleParams()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (conn *Conn) handleAuthResult(cipher []byte) (err error) {
	// Read Result Packet
	if err = conn.readResultOK(); err == nil {
		return // auth successful
	}

	if conn.cfg == nil {
		return // auth failed and retry not possible
	}

	// Retry auth if configured to do so.
	if conn.cfg.AllowOldPasswords && err == ErrOldPassword {
		// Retry with old authentication method. Note: there are edge cases
		// where this should work but doesn't; this is currently "wontfix":
		// https://github.com/go-sql-driver/mysql/issues/184
		if err = conn.writeOldAuthPacket(cipher); err != nil {
			return
		}
		err = conn.readResultOK()
	} else if conn.cfg.AllowCleartextPasswords && err == ErrCleartextPassword {
		// Retry with clear text password for
		// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
		// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
		if err = conn.writeClearAuthPacket(); err != nil {
			return
		}
		err = conn.readResultOK()
	}
	return
}

// Handles parameters set in DSN after the connection is established
func (conn *Conn) handleParams() (err error) {
	for param, val := range conn.cfg.Params {
		switch param {
		// Charset
		case "charset":
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err = conn.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// System Vars
		default:
			err = conn.exec("SET " + param + "=" + val + "")
			if err != nil {
				return
			}
		}
	}
	return
}

func (conn *Conn) Close() (err error) {
	// Makes Close idempotent
	if conn.netConn != nil {
		err = conn.writeCommandPacket(comQuit)
	}

	conn.cleanup()

	return
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because MySQL will have already
// closed the network connection.
func (conn *Conn) cleanup() {
	// Makes cleanup idempotent
	if conn.netConn != nil {
		if err := conn.netConn.Close(); err != nil {
			errLog.Print(err)
		}
		conn.netConn = nil
	}
	conn.cfg = nil
	conn.buf.rd = nil
}

func (conn *Conn) interpolateParams(query string, args []interface{}) (string, error) {
	buf := conn.buf.takeCompleteBuffer()
	if buf == nil {
		// can not take the buffer. Something must be wrong with the connection
		return "", ErrBusyBuffer
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
				v := v.In(conn.cfg.Loc)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if conn.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if conn.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '\'')
		default:
			//fmt.Printf("arg: %#v \n", arg) // DEBUG
			return "", ErrUnsafeInterpolate
		}

		if len(buf)+4 > conn.maxPacketAllowed {
			return "", ErrPktTooLarge // TODO?
		}
	}
	if argPos != len(args) {
		return "", ErrInterpolateFailed // TODO
	}
	return string(buf), nil
}

func (conn *Conn) Exec(query string, args ...interface{}) (res Result, err error) {
	if conn.netConn == nil {
		err = ErrInvalidConn
		return
	}
	if len(args) != 0 {
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		query, err = conn.interpolateParams(query, args)
		if err != nil {
			return
		}
		args = nil
	}
	conn.affectedRows = 0
	conn.insertID = 0

	if err = conn.exec(query); err == nil {
		res.affectedRows = int64(conn.affectedRows)
		res.insertID = int64(conn.insertID)
	}
	return
}

// Internal function to execute commands
func (conn *Conn) exec(query string) error {
	// Send command
	err := conn.writeCommandPacketStr(comQuery, query)
	if err != nil {
		return err
	}

	// Read Result
	resLen, err := conn.readResultSetHeaderPacket()
	if err == nil && resLen > 0 {
		if err = conn.readUntilEOF(); err != nil {
			return err
		}

		err = conn.readUntilEOF()
	}

	return err
}

func (conn *Conn) Query(query string, args ...interface{}) (rows Rows, err error) {
	if conn.netConn == nil {
		return nil, ErrInvalidConn
	}
	if len(args) != 0 {
		// try client-side prepare to reduce roundtrip
		query, err = conn.interpolateParams(query, args)
		if err != nil {
			return
		}
		args = nil
	}
	// Send command
	if err = conn.writeCommandPacketStr(comQuery, query); err == nil {
		// Read Result
		var resLen int
		resLen, err = conn.readResultSetHeaderPacket()
		if err == nil {
			tr := new(textRows)
			tr.conn = conn

			if resLen == 0 {
				// no columns, no more data
				return emptyRows{}, nil
			}
			// Columns
			tr.columns, err = conn.readColumns(resLen)
			return tr, err
		}
	}
	return
}

// Gets the value of the given MySQL System Variable
// The returned byte slice is only valid until the next read
func (conn *Conn) getSystemVar(name string) ([]byte, error) {
	// Send command
	if err := conn.writeCommandPacketStr(comQuery, "SELECT @@"+name); err != nil {
		return nil, err
	}

	// Read Result
	resLen, err := conn.readResultSetHeaderPacket()
	if err == nil {
		tr := new(textRows)
		tr.conn = conn
		tr.columns = []Field{{fieldType: fieldTypeVarChar}}

		if resLen > 0 {
			// Columns
			if err := conn.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		dest := make([]interface{}, resLen)
		if err = tr.readRow(dest); err == nil {
			return dest[0].([]byte), conn.readUntilEOF()
		}
	}
	return nil, err
}
