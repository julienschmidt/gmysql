// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

func (rows *textRows) convert(dest []interface{}) error {
	data := rows.data
	pos := 0

	for i := range dest {
		// Read bytes and convert to string
		val, isNull, n, err := readLengthEncodedString(data[pos:])
		pos += n

		if err != nil {
			return err
		}

		//switch v := dest.(type) {
		switch dest[i].(type) {
		case interface{}:
			if isNull {
				dest[i] = nil
			} else {
				dest[i] = val
			}
		case []byte:
			if isNull {
				dest[i] = 0
			} else {
				dest[i] = val
			}
		case time.Time:
			if isNull {
				dest[i] = time.Time{}
			} else {
				dest[i], err = parseDateTime(
					string(dest[i].([]byte)),
					rows.conn.cfg.Loc,
				)
				if err != nil {
					return err
				}
			}
		default:
			return errors.New("unsupported scan type")
		}
	}
	return nil
}

func (rows *binaryRows) convert(dest []interface{}) error {
	data := rows.data
	pos := 0

	for i := range dest {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((rows.nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		// Convert to byte-coded string
		switch rows.columns[i].fieldType {
		case fieldTypeNULL:
			dest[i] = nil
			continue

		// Numeric Types
		case fieldTypeTiny:
			if rows.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = int64(int8(data[pos]))
			}
			pos++
			continue

		case fieldTypeShort, fieldTypeYear:
			if rows.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue

		case fieldTypeInt24, fieldTypeLong:
			if rows.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue

		case fieldTypeLongLong:
			if rows.columns[i].flags&flagUnsigned != 0 {
				val := binary.LittleEndian.Uint64(data[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8
			continue

		case fieldTypeFloat:
			dest[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4])))
			pos += 4
			continue

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry:
			var isNull bool
			var n int
			var err error
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			fieldTypeDate, fieldTypeNewDate, // Date YYYY-MM-DD
			fieldTypeTime,                         // Time [-][H]HH:MM:SS[.fractal]
			fieldTypeTimestamp, fieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			var err error
			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			switch {
			case isNull:
				dest[i] = nil
				continue
			case rows.columns[i].fieldType == fieldTypeTime:
				// database/sql does not support an equivalent to TIME, return a string
				var dstlen uint8
				switch decimals := rows.columns[i].decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return fmt.Errorf(
						"MySQL protocol error, illegal decimals value %d",
						rows.columns[i].decimals,
					)
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen, true)
			//case rows.conn.parseTime:
			//	dest[i], err = parseBinaryDateTime(num, data[pos:], rows.conn.cfg.Loc)
			default:
				var dstlen uint8
				if rows.columns[i].fieldType == fieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := rows.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return fmt.Errorf(
							"MySQL protocol error, illegal decimals value %d",
							rows.columns[i].decimals,
						)
					}
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen, false)
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return err
			}

		// Please report if this happens!
		default:
			return fmt.Errorf("Unknown FieldType %d", rows.columns[i].fieldType)
		}
	}
	return nil
}
