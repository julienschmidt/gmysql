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
	"log"
	"testing"
)

func TestErrorsSetLogger(t *testing.T) {
	previous := errLog
	defer func() {
		errLog = previous
	}()

	// set up logger
	const expected = "prefix: test\n"
	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(buffer, "prefix: ", 0)

	// print
	SetLogger(logger)
	errLog.Print("test")

	// check result
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestErrorsStrictIgnoreNotes(t *testing.T) {
	runTests(t, dsn+"&sql_notes=false", func(ct *ConnTest) {
		ct.mustExec("DROP TABLE IF EXISTS does_not_exist")
	})
}
