// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

type TB testing.B

func (tb *TB) check(err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func (tb *TB) checkConn(conn *Conn, err error) *Conn {
	tb.check(err)
	return conn
}

func (tb *TB) checkRows(rows Rows, err error) Rows {
	tb.check(err)
	return rows
}

func (tb *TB) checkStmt(stmt *Stmt, err error) *Stmt {
	tb.check(err)
	return stmt
}

func initConn(b *testing.B, queries ...string) *Conn {
	tb := (*TB)(b)
	conn := tb.checkConn(Open(dsn))
	for _, query := range queries {
		if _, err := conn.Exec(query); err != nil {
			if w, ok := err.(Warnings); ok {
				b.Logf("Warning on %q: %v", query, w)
			} else {
				b.Fatalf("Error on %q: %v", query, err)
			}
		}
	}
	return conn
}

func BenchmarkQuery(b *testing.B) {
	tb := (*TB)(b)

	conn := initConn(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer conn.Close()

	stmt := tb.checkStmt(conn.Prepare("SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var got string
		// TODO
		//tb.check(stmt.QueryRow(1).Scan(&got))
		rows := tb.checkRows(stmt.Query(1))
		if !rows.Next() {
			b.Fatal("received no row")
		}
		tb.check(rows.Scan(&got))
		if rows.Next() {
			b.Fatal("received more than one row")
		}
		if got != "one" {
			b.Errorf("query = %q; want one", got)
			return
		}
	}
}

func BenchmarkExec(b *testing.B) {
	tb := (*TB)(b)

	conn := tb.checkConn(Open(dsn))
	defer conn.Close()

	stmt := tb.checkStmt(conn.Prepare("DO 1"))
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(); err != nil {
			b.Fatal(err.Error())
		}
	}
}

// data, but no db writes
var roundtripSample []byte

func initRoundtripBenchmarks() ([]byte, int, int) {
	if roundtripSample == nil {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return roundtripSample, 16, len(roundtripSample)
}

func BenchmarkRoundtripTxt(b *testing.B) {
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)

	tb := (*TB)(b)
	conn := tb.checkConn(Open(dsn))
	defer conn.Close()

	b.ReportAllocs()
	b.ResetTimer()

	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sampleString[0:length]
		rows := tb.checkRows(conn.Query(`SELECT "` + test + `"`))

		if !rows.Next() {
			b.Fatal("received no row")
		}
		tb.check(rows.Scan(&result))
		if rows.Next() {
			b.Fatal("received more than one row")
		}
		if result != test {
			rows.Close()
			b.Error("mismatch")
		}
	}
}

func BenchmarkRoundtripBin(b *testing.B) {
	sample, min, max := initRoundtripBenchmarks()

	tb := (*TB)(b)
	conn := tb.checkConn(Open(dsn))
	defer conn.Close()

	stmt := tb.checkStmt(conn.Prepare("SELECT ?"))
	defer stmt.Close()

	b.ReportAllocs()
	b.ResetTimer()

	var result []byte // TODO RawBytes
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sample[0:length]
		rows := tb.checkRows(stmt.Query(test))

		if !rows.Next() {
			b.Fatal("received no row")
		}
		tb.check(rows.Scan(&result))
		if rows.Next() {
			b.Fatal("received more than one row")
		}
		if !bytes.Equal(result, test) {
			rows.Close()
			b.Error("mismatch")
		}
	}
}

func BenchmarkInterpolation(b *testing.B) {
	mc := &Conn{
		cfg: &Config{
			Loc: time.UTC,
		},
		maxPacketAllowed: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		buf:              newBuffer(nil),
	}

	args := []interface{}{
		int64(42424242),
		float64(math.Pi),
		false,
		time.Unix(1423411542, 807015000),
		[]byte("bytes containing special chars ' \" \a \x00"),
		"string containing special chars ' \" \a \x00",
	}
	q := "SELECT ?, ?, ?, ?, ?, ?"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mc.interpolateParams(q, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}
