// gmysql - A MySQL package for Go
//
// Copyright 2016 The gmysql Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build appengine

package gmysql

import (
	"appengine/cloudsql"
)

func init() {
	RegisterDial("cloudsql", cloudsql.Dial)
}
