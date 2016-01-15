// gmysql - A MySQL package for Go
//
// Copyright 2016 The gmysql Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

// Packets documentation:
// http://dev.mysql.com/doc/internals/en/client-server-protocol.html

// Read packet to buffer 'data'
func (conn *Conn) readPacket() ([]byte, error) {
	var payload []byte
	for {
		// Read packet header
		data, err := conn.buf.readNext(4)
		if err != nil {
			conn.Close()
			return nil, err
		}

		// Packet Length [24 bit]
		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

		if pktLen < 1 {
			conn.Close()
			return nil, ErrMalformPkt
		}

		// Check Packet Sync [8 bit]
		if data[3] != conn.sequence {
			if data[3] > conn.sequence {
				return nil, ErrPktSyncMul
			}
			return nil, ErrPktSync
		}
		conn.sequence++

		// Read packet body [pktLen bytes]
		data, err = conn.buf.readNext(pktLen)
		if err != nil {
			conn.Close()
			return nil, err
		}

		isLastPacket := (pktLen < maxPacketSize)

		// Zero allocations for non-splitting packets
		if isLastPacket && payload == nil {
			return data, nil
		}

		payload = append(payload, data...)

		if isLastPacket {
			return payload, nil
		}
	}
}

// Write packet buffer 'data'
func (conn *Conn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	if pktLen > conn.maxPacketAllowed {
		return ErrPktTooLarge
	}

	for {
		var size int
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = conn.sequence

		// Write packet
		n, err := conn.netConn.Write(data[:4+size])
		if err == nil && n == 4+size {
			conn.sequence++
			if size != maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		// Handle error
		if err != nil {
			return err
		}
		return ErrMalformPkt
	}
}

/******************************************************************************
*                           Initialisation Process                            *
******************************************************************************/

// Handshake Initialization Packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (conn *Conn) readInitPacket() ([]byte, error) {
	data, err := conn.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == iERR {
		return nil, conn.handleErrorPacket(data)
	}

	// protocol version [1 byte]
	if data[0] < minProtocolVersion {
		return nil, fmt.Errorf(
			"unsupported MySQL Protocol Version %d. Protocol Version %d or higher is required",
			data[0],
			minProtocolVersion,
		)
	}

	// server version [null terminated string]
	// connection id [4 bytes]
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	// first part of the password cipher [8 bytes]
	cipher := data[pos : pos+8]

	// (filler) always 0x00 [1 byte]
	pos += 8 + 1

	// capability flags (lower 2 bytes) [2 bytes]
	conn.flags = clientFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	if conn.flags&clientProtocol41 == 0 {
		return nil, ErrOldProtocol
	}
	if conn.flags&clientSSL == 0 && conn.cfg.TLS != nil {
		return nil, ErrNoTLS
	}
	pos += 2

	if len(data) > pos {
		// character set [1 byte]
		// status flags [2 bytes]
		// capability flags (upper 2 bytes) [2 bytes]
		// length of auth-plugin-data [1 byte]
		// reserved (all [00]) [10 bytes]
		pos += 1 + 2 + 2 + 1 + 10

		// second part of the password cipher [mininum 13 bytes],
		// where len=MAX(13, length of auth-plugin-data - 8)
		//
		// The web documentation is ambiguous about the length. However,
		// according to mysql-5.7/sql/auth/sql_authentication.cc line 538,
		// the 13th byte is "\0 byte, terminating the second part of
		// a scramble". So the second part of the password cipher is
		// a NULL terminated string that's at least 13 bytes with the
		// last byte being NULL.
		//
		// The official Python library uses the fixed length 12
		// which seems to work but technically could have a hidden bug.
		cipher = append(cipher, data[pos:pos+12]...)

		// TODO: Verify string termination
		// EOF if version (>= 5.5.7 and < 5.5.10) or (>= 5.6.0 and < 5.6.2)
		// \NUL otherwise
		//
		//if data[len(data)-1] == 0 {
		//	return
		//}
		//return ErrMalformPkt

		// make a memory safe copy of the cipher slice
		var b [20]byte
		copy(b[:], cipher)
		return b[:], nil
	}

	// make a memory safe copy of the cipher slice
	var b [8]byte
	copy(b[:], cipher)
	return b[:], nil
}

// Client Authentication Packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (conn *Conn) writeAuthPacket(cipher []byte) error {
	// Adjust client flags based on server support
	clientFlags := clientProtocol41 |
		clientSecureConn |
		clientLongPassword |
		clientTransactions |
		clientLocalFiles |
		clientPluginAuth |
		conn.flags&clientLongFlag

	if conn.cfg.ClientFoundRows {
		clientFlags |= clientFoundRows
	}

	// To enable TLS / SSL
	if conn.cfg.TLS != nil {
		clientFlags |= clientSSL
	}

	// User Password
	scrambleBuff := scramblePassword(cipher, []byte(conn.cfg.Passwd))

	pktLen := 4 + 4 + 1 + 23 + len(conn.cfg.User) + 1 + 1 + len(scrambleBuff) + 21 + 1

	// To specify a db name
	if n := len(conn.cfg.DBName); n > 0 {
		clientFlags |= clientConnectWithDB
		pktLen += n + 1
	}

	// Calculate packet length and get buffer with that size
	data := conn.buf.takeSmallBuffer(pktLen + 4)
	if data == nil {
		return ErrBusyBuffer
	}

	// ClientFlags [32 bit]
	data[4] = byte(clientFlags)
	data[5] = byte(clientFlags >> 8)
	data[6] = byte(clientFlags >> 16)
	data[7] = byte(clientFlags >> 24)

	// MaxPacketSize [32 bit] (none)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	// Charset [1 byte]
	data[12] = conn.cfg.Collation

	// SSL Connection Request Packet
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if conn.cfg.TLS != nil {
		// Send TLS / SSL request packet
		if err := conn.writePacket(data[:(4+4+1+23)+4]); err != nil {
			return err
		}

		// Switch to TLS
		tlsConn := tls.Client(conn.netConn, conn.cfg.TLS)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		conn.netConn = tlsConn
		conn.buf.rd = tlsConn
	}

	// Filler [23 bytes] (all 0x00)
	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	// User [null terminated string]
	if len(conn.cfg.User) > 0 {
		pos += copy(data[pos:], conn.cfg.User)
	}
	data[pos] = 0x00
	pos++

	// ScrambleBuffer [length encoded integer]
	data[pos] = byte(len(scrambleBuff))
	pos += 1 + copy(data[pos+1:], scrambleBuff)

	// Databasename [null terminated string]
	if len(conn.cfg.DBName) > 0 {
		pos += copy(data[pos:], conn.cfg.DBName)
		data[pos] = 0x00
		pos++
	}

	// Assume native client during response
	pos += copy(data[pos:], "mysql_native_password")
	data[pos] = 0x00

	// Send Auth packet
	return conn.writePacket(data)
}

//  Client old authentication packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (conn *Conn) writeOldAuthPacket(cipher []byte) error {
	// User password
	scrambleBuff := scrambleOldPassword(cipher, []byte(conn.cfg.Passwd))

	// Calculate the packet length and add a tailing 0
	pktLen := len(scrambleBuff) + 1
	data := conn.buf.takeSmallBuffer(4 + pktLen)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// Add the scrambled password [null terminated string]
	copy(data[4:], scrambleBuff)
	data[4+pktLen-1] = 0x00

	return conn.writePacket(data)
}

//  Client clear text authentication packet
// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse
func (conn *Conn) writeClearAuthPacket() error {
	// Calculate the packet length and add a tailing 0
	pktLen := len(conn.cfg.Passwd) + 1
	data := conn.buf.takeSmallBuffer(4 + pktLen)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// Add the clear password [null terminated string]
	copy(data[4:], conn.cfg.Passwd)
	data[4+pktLen-1] = 0x00

	return conn.writePacket(data)
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

func (conn *Conn) writeCommandPacket(command byte) error {
	// Reset Packet Sequence
	conn.sequence = 0

	data := conn.buf.takeSmallBuffer(4 + 1)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// Add command byte
	data[4] = command

	// Send CMD packet
	return conn.writePacket(data)
}

func (conn *Conn) writeCommandPacketStr(command byte, arg string) error {
	// Reset Packet Sequence
	conn.sequence = 0

	pktLen := 1 + len(arg)
	data := conn.buf.takeBuffer(pktLen + 4)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// Add command byte
	data[4] = command

	// Add arg
	copy(data[5:], arg)

	// Send CMD packet
	return conn.writePacket(data)
}

func (conn *Conn) writeCommandPacketUint32(command byte, arg uint32) error {
	// Reset Packet Sequence
	conn.sequence = 0

	data := conn.buf.takeSmallBuffer(4 + 1 + 4)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// Add command byte
	data[4] = command

	// Add arg [32 bit]
	data[5] = byte(arg)
	data[6] = byte(arg >> 8)
	data[7] = byte(arg >> 16)
	data[8] = byte(arg >> 24)

	// Send CMD packet
	return conn.writePacket(data)
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Returns error if Packet is not an 'Result OK'-Packet
func (conn *Conn) readResultOK() error {
	data, err := conn.readPacket()
	if err == nil {
		// packet indicator
		switch data[0] {

		case iOK:
			return conn.handleOkPacket(data)

		case iEOF:
			if len(data) > 1 {
				plugin := string(data[1:bytes.IndexByte(data, 0x00)])
				if plugin == "mysql_old_password" {
					// using old_passwords
					return ErrOldPassword
				} else if plugin == "mysql_clear_password" {
					// using clear text password
					return ErrCleartextPassword
				} else {
					return ErrUnknownPlugin
				}
			} else {
				return ErrOldPassword
			}

		default: // Error otherwise
			return conn.handleErrorPacket(data)
		}
	}
	return err
}

// Result Set Header Packet
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (conn *Conn) readResultSetHeaderPacket() (int, error) {
	data, err := conn.readPacket()
	if err == nil {
		switch data[0] {

		case iOK:
			return 0, conn.handleOkPacket(data)

		case iERR:
			return 0, conn.handleErrorPacket(data)

		case iLocalInFile:
			return 0, conn.handleInFileRequest(string(data[1:]))
		}

		// column count
		num, _, n := readLengthEncodedInteger(data)
		if n-len(data) == 0 {
			return int(num), nil
		}

		return 0, ErrMalformPkt
	}
	return 0, err
}

// Error Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet
func (conn *Conn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return ErrMalformPkt
	}

	// 0xff [1 byte]

	// Error Number [16 bit uint]
	errno := binary.LittleEndian.Uint16(data[1:3])

	pos := 3

	// SQL State [optional: # + 5bytes string]
	if data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}

	// Error Message [string]
	return &Error{
		Number:  errno,
		Message: string(data[pos:]),
	}
}

// Ok Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet
func (conn *Conn) handleOkPacket(data []byte) error {
	var n, m int

	// 0x00 [1 byte]

	// Affected rows [Length Coded Binary]
	conn.affectedRows, _, n = readLengthEncodedInteger(data[1:])

	// Insert ID [Length Coded Binary]
	conn.insertID, _, m = readLengthEncodedInteger(data[1+n:])

	// server_status [2 bytes]
	conn.status = statusFlag(data[1+n+m]) | statusFlag(data[1+n+m+1])<<8

	// warning count [2 bytes]
	if !conn.strict {
		return nil
	}
	pos := 1 + n + m + 2
	if binary.LittleEndian.Uint16(data[pos:pos+2]) > 0 {
		return conn.getWarnings()
	}
	return nil
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
func (conn *Conn) readColumns(count int) ([]Field, error) {
	columns := make([]Field, count)

	for i := 0; ; i++ {
		data, err := conn.readPacket()
		if err != nil {
			return nil, err
		}

		// EOF Packet
		if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
			if i == count {
				return columns, nil
			}
			return nil, fmt.Errorf("ColumnsCount mismatch n:%d len:%d", count, len(columns))
		}

		// Catalog
		pos, err := skipLengthEncodedString(data)
		if err != nil {
			return nil, err
		}

		// Database [len coded string]
		n, err := skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Table [len coded string]
		if conn.cfg.ColumnsWithAlias {
			tableName, _, n, err := readLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			columns[i].tableName = string(tableName)
		} else {
			n, err = skipLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
		}

		// Original table [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Name [len coded string]
		name, _, n, err := readLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		columns[i].name = string(name)
		pos += n

		// Original name [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}

		// Filler [uint8]
		// Charset [charset, collation uint8]
		// Length [uint32]
		pos += n + 1 + 2 + 4

		// Field type [uint8]
		columns[i].fieldType = data[pos]
		pos++

		// Flags [uint16]
		columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2

		// Decimals [uint8]
		columns[i].decimals = data[pos]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
		//}
	}
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (rows *textRows) readRow() error {
	conn := rows.conn

	data, err := conn.readPacket()
	if err != nil {
		return err
	}

	// EOF Packet
	if data[0] == iEOF && len(data) == 5 {
		rows.conn = nil
		return io.EOF
	}
	if data[0] == iERR {
		rows.conn = nil
		return conn.handleErrorPacket(data)
	}

	rows.data = data
	return nil
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (conn *Conn) readUntilEOF() error {
	for {
		data, err := conn.readPacket()

		// No Err and no EOF Packet
		if err == nil && data[0] != iEOF {
			continue
		}
		return err // Err or EOF
	}
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

// Prepare Result Packets
// http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
func (stmt *Stmt) readPrepareResultPacket() (uint16, error) {
	data, err := stmt.conn.readPacket()
	if err == nil {
		// packet indicator [1 byte]
		if data[0] != iOK {
			return 0, stmt.conn.handleErrorPacket(data)
		}

		// statement id [4 bytes]
		stmt.id = binary.LittleEndian.Uint32(data[1:5])

		// Column count [16 bit uint]
		columnCount := binary.LittleEndian.Uint16(data[5:7])

		// Param count [16 bit uint]
		stmt.paramCount = int(binary.LittleEndian.Uint16(data[7:9]))

		// Reserved [8 bit]

		// Warning count [16 bit uint]
		if !stmt.conn.strict {
			return columnCount, nil
		}
		// Check for warnings count > 0, only available in MySQL > 4.1
		if len(data) >= 12 && binary.LittleEndian.Uint16(data[10:12]) > 0 {
			return columnCount, stmt.conn.getWarnings()
		}
		return columnCount, nil
	}
	return 0, err
}

// http://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
func (stmt *Stmt) writeCommandLongData(paramID int, arg []byte) error {
	maxLen := stmt.conn.maxPacketAllowed - 1
	pktLen := maxLen

	// After the header (bytes 0-3) follows before the data:
	// 1 byte command
	// 4 bytes stmtID
	// 2 bytes paramID
	const dataOffset = 1 + 4 + 2

	// Can not use the write buffer since
	// a) the buffer is too small
	// b) it is in use
	data := make([]byte, 4+1+4+2+len(arg))

	copy(data[4+dataOffset:], arg)

	for argLen := len(arg); argLen > 0; argLen -= pktLen - dataOffset {
		if dataOffset+argLen < maxLen {
			pktLen = dataOffset + argLen
		}

		stmt.conn.sequence = 0
		// Add command byte [1 byte]
		data[4] = comStmtSendLongData

		// Add stmtID [32 bit]
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)

		// Add paramID [16 bit]
		data[9] = byte(paramID)
		data[10] = byte(paramID >> 8)

		// Send CMD packet
		err := stmt.conn.writePacket(data[:4+pktLen])
		if err == nil {
			data = data[pktLen-dataOffset:]
			continue
		}
		return err

	}

	// Reset Packet Sequence
	stmt.conn.sequence = 0
	return nil
}

// Execute Prepared Statement
// http://dev.mysql.com/doc/internals/en/com-stmt-execute.html
func (stmt *Stmt) writeExecutePacket(args []interface{}) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"Arguments count mismatch (Got: %d Has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 4 + 1 + 4 + 1 + 4
	conn := stmt.conn

	// Reset packet-sequence
	conn.sequence = 0

	var data []byte

	if len(args) == 0 {
		data = conn.buf.takeBuffer(minPktLen)
	} else {
		data = conn.buf.takeCompleteBuffer()
	}
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		return ErrBusyBuffer
	}

	// command [1 byte]
	data[4] = comStmtExecute

	// statement_id [4 bytes]
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00

	if len(args) > 0 {
		pos := minPktLen

		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= len(data) {
			// buffer has to be extended but we don't know by how much so
			// we depend on append after all data with known sizes fit.
			// We stop at that because we deal with a lot of columns here
			// which makes the required allocation size hard to guess.
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := 0; i < maskLen; i++ {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of each parameter [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// value of each parameter [n bytes]
		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			// build NULL-bitmap
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = fieldTypeNULL
				paramTypes[i+i+1] = 0x00
				continue
			}

			// cache types and values
			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = fieldTypeLongLong
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(uint64(v))...,
					)
				}

			case float64:
				paramTypes[i+i] = fieldTypeDouble
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						math.Float64bits(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(math.Float64bits(v))...,
					)
				}

			case bool:
				paramTypes[i+i] = fieldTypeTiny
				paramTypes[i+i+1] = 0x00

				if v {
					paramValues = append(paramValues, 0x01)
				} else {
					paramValues = append(paramValues, 0x00)
				}

			case []byte:
				// Common case (non-nil value) first
				if v != nil {
					paramTypes[i+i] = fieldTypeString
					paramTypes[i+i+1] = 0x00

					if len(v) < conn.maxPacketAllowed-pos-len(paramValues)-(len(args)-(i+1))*64 {
						paramValues = appendLengthEncodedInteger(paramValues,
							uint64(len(v)),
						)
						paramValues = append(paramValues, v...)
					} else {
						if err := stmt.writeCommandLongData(i, v); err != nil {
							return err
						}
					}
					continue
				}

				// Handle []byte(nil) as a NULL value
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = fieldTypeNULL
				paramTypes[i+i+1] = 0x00

			case string:
				paramTypes[i+i] = fieldTypeString
				paramTypes[i+i+1] = 0x00

				if len(v) < conn.maxPacketAllowed-pos-len(paramValues)-(len(args)-(i+1))*64 {
					paramValues = appendLengthEncodedInteger(paramValues,
						uint64(len(v)),
					)
					paramValues = append(paramValues, v...)
				} else {
					if err := stmt.writeCommandLongData(i, []byte(v)); err != nil {
						return err
					}
				}

			case time.Time:
				paramTypes[i+i] = fieldTypeString
				paramTypes[i+i+1] = 0x00

				var val []byte
				if v.IsZero() {
					val = []byte("0000-00-00")
				} else {
					val = []byte(v.In(conn.cfg.Loc).Format(timeFormat))
				}

				paramValues = appendLengthEncodedInteger(paramValues,
					uint64(len(val)),
				)
				paramValues = append(paramValues, val...)

			default:
				return fmt.Errorf("Can't convert type: %T", arg)
			}
		}

		// Check if param values exceeded the available buffer
		// In that case we must build the data packet with the new values buffer
		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
			conn.buf.buf = data
		}

		pos += len(paramValues)
		data = data[:pos]
	}

	return conn.writePacket(data)
}

// http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (rows *binaryRows) readRow() error {
	data, err := rows.conn.readPacket()
	if err != nil {
		return err
	}

	// packet indicator [1 byte]
	if data[0] != iOK {
		rows.conn = nil
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			return io.EOF
		}

		// Error otherwise
		return rows.conn.handleErrorPacket(data)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(rows.columns)+7+2)>>3
	rows.nullMask = data[1:pos]

	rows.data = data
	return nil
}
