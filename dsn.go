// gmysql - A MySQL package for Go
//
// Copyright 2016 The gmysql Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package gmysql

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

var (
	errInvalidDSNUnescaped       = errors.New("invalid DSN: Did you forget to escape a param value?")
	errInvalidDSNAddr            = errors.New("invalid DSN: Network Address not terminated (missing closing brace)")
	errInvalidDSNNoSlash         = errors.New("invalid DSN: Missing the slash separating the database name")
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can be used with ascii, latin1, utf8 and utf8mb4 charset")
)

// Config is a configuration parsed from a DSN string
type Config struct {
	User      string            // Username
	Passwd    string            // Password
	Net       string            // Network type
	Addr      string            // Network address
	DBName    string            // Database name
	Params    map[string]string // Connection parameters
	Loc       *time.Location    // Location for time.Time values
	TLS       *tls.Config       // TLS configuration
	Timeout   time.Duration     // Dial timeout
	Collation uint8             // Connection collation

	AllowAllFiles           bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	AllowCleartextPasswords bool // Allows the cleartext client side plugin
	AllowOldPasswords       bool // Allows the old insecure password method
	ClientFoundRows         bool // Return number of matching rows instead of rows changed
	ColumnsWithAlias        bool // Prepend table alias to column names
	Strict                  bool // Return warnings as errors
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = &Config{
		Loc:       time.UTC,
		Collation: defaultCollation,
	}

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	if unsafeCollations[cfg.Collation] { // TODO
		return nil, errInvalidDSNUnsafeCollation
	}

	// Set default network if empty
	if cfg.Net == "" {
		cfg.Net = "tcp"
	}

	// Set default address if empty
	if cfg.Addr == "" {
		switch cfg.Net {
		case "tcp":
			cfg.Addr = "127.0.0.1:3306"
		case "unix":
			cfg.Addr = "/tmp/mysql.sock"
		default:
			return nil, errors.New("Default addr for network '" + cfg.Net + "' unknown")
		}

	}

	return
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {

		// Disable INFILE whitelist / enable all files
		case "allowAllFiles":
			var isBool bool
			cfg.AllowAllFiles, isBool = readBool(value)
			if !isBool {
				return fmt.Errorf("Invalid Bool value: %s", value)
			}

		// Use cleartext authentication mode (MySQL 5.5.10+)
		case "allowCleartextPasswords":
			var isBool bool
			cfg.AllowCleartextPasswords, isBool = readBool(value)
			if !isBool {
				return fmt.Errorf("Invalid Bool value: %s", value)
			}

		// Use old authentication mode (pre MySQL 4.1)
		case "allowOldPasswords":
			var isBool bool
			cfg.AllowOldPasswords, isBool = readBool(value)
			if !isBool {
				return fmt.Errorf("Invalid Bool value: %s", value)
			}

		// Switch "rowsAffected" mode
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = readBool(value)
			if !isBool {
				return fmt.Errorf("Invalid Bool value: %s", value)
			}

		// Collation
		case "collation":
			collation, ok := collations[value]
			if !ok {
				// Note possibility for false negatives:
				// could be triggered  although the collation is valid if the
				// collations map does not contain entries the server supports.
				err = errors.New("unknown collation")
				return
			}
			cfg.Collation = collation
			break

		case "columnsWithAlias":
			var isBool bool
			cfg.ColumnsWithAlias, isBool = readBool(value)
			if !isBool {
				return fmt.Errorf("Invalid Bool value: %s", value)
			}

		// Compression
		case "compress":
			return errors.New("Compression not implemented yet")

		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}

		// Strict mode
		case "strict":
			var isBool bool
			cfg.Strict, isBool = readBool(value)
			if !isBool {
				return errors.New("Invalid Bool value: " + value)
			}

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			boolValue, isBool := readBool(value)
			if isBool {
				if boolValue {
					cfg.TLS = &tls.Config{}
				}
			} else if value, err := url.QueryUnescape(value); err != nil {
				return fmt.Errorf("Invalid value for tls config name: %v", err)
			} else {
				if strings.ToLower(value) == "skip-verify" {
					cfg.TLS = &tls.Config{InsecureSkipVerify: true}
				} else if tlsConfig, ok := tlsConfigRegister[value]; ok {
					if len(tlsConfig.ServerName) == 0 && !tlsConfig.InsecureSkipVerify {
						host, _, err := net.SplitHostPort(cfg.Addr)
						if err == nil {
							tlsConfig.ServerName = host
						}
					}

					cfg.TLS = tlsConfig
				} else {
					return fmt.Errorf("Invalid value / unknown config name: %s", value)
				}
			}

		default:
			// lazy init
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}
