// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

type Result struct {
	affectedRows int64
	insertID     int64
}

func (res *Result) LastInsertID() (int64, error) {
	return res.insertID, nil
}

func (res *Result) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
