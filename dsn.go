package mdb

import (
	"bytes"
	"crypto/rsa"
	"crypto/tls"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidDSNUnescaped       = errors.New("invalid DSN: did you forget to escape a param value?")
	errInvalidDSNAddr            = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash         = errors.New("invalid DSN: missing the slash separating the database name")
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can not be used with unsafe collations")
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	User             string            // Username
	Password         string            // Password (requires User)
	Net              string            // Network type
	Addr             string            // Network address (requires Net)
	DBName           string            // Database name
	Params           map[string]string // Connection parameters
	// TODO: Add Support
	//Collation        string            // Connection collation
	Loc              *time.Location    // Location for time.Time values
	MaxAllowedPacket int               // Max packet size allowed
	ServerPubKey     string            // Server public key name
	pubKey           *rsa.PublicKey    // Server public key
	TLSConfig        string            // TLS configuration name
	tls              *tls.Config       // TLS configuration
	Timeout          time.Duration     // Dial timeout
	ReadTimeout      time.Duration     // I/O read timeout
	WriteTimeout     time.Duration     // I/O write timeout

	// TODO: Add Support
	//AllowAllFiles           bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	CheckConnLiveness       bool // Check connections for liveness before using them
	ClientFoundRows         bool // Return number of matching rows instead of rows changed
	//ColumnsWithAlias        bool // Prepend table alias to column names
	InterpolateParams       bool // Interpolate placeholders into query string
	//MultiStatements         bool // Allow multiple statements in one query
	MaxRowCount,
	FetchSize int32
	ParseTime               bool // Parse time values to time.Time
	RejectReadOnly          bool // Reject read-only connections
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &Config{
		//Collation:            defaultCollation,
		Loc:                  time.UTC,
		MaxAllowedPacket:     defaultMaxAllowedPacket,
		CheckConnLiveness:    true,
		FetchSize: DEFAULT_BATCH_SIZE,
		MaxRowCount: DEFAULT_MAX_ROWS,
	}
}

func (cfg *Config) Clone() *Config {
	var cp = *cfg

	if cp.tls != nil {
		cp.tls = cfg.tls.Clone()
	}
	if len(cp.Params) > 0 {
		cp.Params = make(map[string]string, len(cfg.Params))
		for k, v := range cfg.Params {
			cp.Params[k] = v
		}
	}
	if cfg.pubKey != nil {
		cp.pubKey = &rsa.PublicKey{
			N: new(big.Int).Set(cfg.pubKey.N),
			E: cfg.pubKey.E,
		}
	}
	return &cp
}

func (cfg *Config) normalize() error {
	//if cfg.InterpolateParams && unsafeCollations[cfg.Collation] {
	//	return errInvalidDSNUnsafeCollation
	//}

	// Set default network if empty
	if cfg.Net == "" {
		cfg.Net = "tcp"
	}

	// Set default address if empty
	if cfg.Addr == "" {
		switch cfg.Net {
		case "tcp":
			cfg.Addr = fmt.Sprintf("127.0.0.1:%s", DEFAULT_ADDR_PORT)
		case "unix":
			cfg.Addr = "/tmp/mdb.sock"
		default:
			return errors.New("default addr for network '" + cfg.Net + "' unknown")
		}
	} else if cfg.Net == "tcp" {
		cfg.Addr = ensureHavePort(cfg.Addr)
	}

	switch cfg.TLSConfig {
	case "false", "":
		// don't set anything
	case "true":
		cfg.tls = &tls.Config{}
	case "skip-verify", "preferred":
		cfg.tls = &tls.Config{InsecureSkipVerify: true}
	default:
		cfg.tls = getTLSConfigClone(cfg.TLSConfig)
		if cfg.tls == nil {
			return errors.New("invalid value / unknown config name: " + cfg.TLSConfig)
		}
	}

	if cfg.tls != nil && cfg.tls.ServerName == "" && !cfg.tls.InsecureSkipVerify {
		host, _, err := net.SplitHostPort(cfg.Addr)
		if err == nil {
			cfg.tls.ServerName = host
		}
	}

	if cfg.ServerPubKey != "" {
		cfg.pubKey = getServerPubKey(cfg.ServerPubKey)
		if cfg.pubKey == nil {
			return errors.New("invalid value / unknown server pub key name: " + cfg.ServerPubKey)
		}
	}

	return nil
}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func (cfg *Config) FormatDSN() string {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(cfg.User) > 0 {
		buf.WriteString(cfg.User)
		if len(cfg.Password) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Password)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(cfg.Net) > 0 {
		buf.WriteString(cfg.Net)
		if len(cfg.Addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(cfg.Addr)
			buf.WriteByte(')')
		}
	}

	// /dbname
	buf.WriteByte('/')
	buf.WriteString(cfg.DBName)

	// [?param1=value1&...&paramN=valueN]
	hasParam := false

	//if cfg.AllowAllFiles {
	//	hasParam = true
	//	buf.WriteString("?allowAllFiles=true")
	//}

	if !cfg.CheckConnLiveness {
		writeDSNParam(&buf, &hasParam, "checkConnLiveness", "false")
	}

	if cfg.ClientFoundRows {
		writeDSNParam(&buf, &hasParam, "clientFoundRows", "true")
	}

	//if col := cfg.Collation; col != defaultCollation && len(col) > 0 {
	//	writeDSNParam(&buf, &hasParam, "collation", col)
	//}

	//if cfg.ColumnsWithAlias {
	//	writeDSNParam(&buf, &hasParam, "columnsWithAlias", "true")
	//}

	if cfg.InterpolateParams {
		writeDSNParam(&buf, &hasParam, "interpolateParams", "true")
	}

	if cfg.Loc != time.UTC && cfg.Loc != nil {
		writeDSNParam(&buf, &hasParam, "loc", url.QueryEscape(cfg.Loc.String()))
	}

	if cfg.ParseTime {
		writeDSNParam(&buf, &hasParam, "parseTime", "true")
	}

	if cfg.ReadTimeout > 0 {
		writeDSNParam(&buf, &hasParam, "readTimeout", cfg.ReadTimeout.String())
	}

	if cfg.RejectReadOnly {
		writeDSNParam(&buf, &hasParam, "rejectReadOnly", "true")
	}

	if len(cfg.ServerPubKey) > 0 {
		writeDSNParam(&buf, &hasParam, "serverPubKey", url.QueryEscape(cfg.ServerPubKey))
	}

	if cfg.Timeout > 0 {
		writeDSNParam(&buf, &hasParam, "timeout", cfg.Timeout.String())
	}

	if len(cfg.TLSConfig) > 0 {
		writeDSNParam(&buf, &hasParam, "tls", url.QueryEscape(cfg.TLSConfig))
	}

	if cfg.WriteTimeout > 0 {
		writeDSNParam(&buf, &hasParam, "writeTimeout", cfg.WriteTimeout.String())
	}

	if cfg.MaxAllowedPacket != defaultMaxAllowedPacket {
		writeDSNParam(&buf, &hasParam, "maxAllowedPacket", strconv.Itoa(cfg.MaxAllowedPacket))
	}

	// other params
	if cfg.Params != nil {
		var params []string
		for param := range cfg.Params {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			writeDSNParam(&buf, &hasParam, param, url.QueryEscape(cfg.Params[param]))
		}
	}

	return buf.String()
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = NewConfig()

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
								cfg.Password = dsn[k+1 : j]
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

	if err = cfg.normalize(); err != nil {
		return nil, err
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
		//// Disable INFILE allowlist / enable all files
		//case "allowAllFiles":
		//	var isBool bool
		//	cfg.AllowAllFiles, isBool = parseBool(value)
		//	if !isBool {
		//		return errors.New("invalid bool value: " + value)
		//	}

		// Check connections for Liveness before using them
		case "checkConnLiveness":
			var isBool bool
			cfg.CheckConnLiveness, isBool = parseBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Switch "rowsAffected" mode
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = parseBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		case "maxRowCount":
			var inter int64
			inter, err = strconv.ParseInt(value, 10, 32)
			if err != nil {
				return errors.New("invalid maxrowcount value: " + value)
			}
			cfg.MaxRowCount = int32(inter)

		case "fetchSize":
			var inter int64
			inter, err = strconv.ParseInt(value, 10, 32)
			if err != nil {
				return errors.New("invalid fetchsize value: " + value)
			}
			cfg.FetchSize = int32(inter)

		//// Collation
		//case "collation":
		//	cfg.Collation = value
		//	break
		//
		//case "columnsWithAlias":
		//	var isBool bool
		//	cfg.ColumnsWithAlias, isBool = parseBool(value)
		//	if !isBool {
		//		return errors.New("invalid bool value: " + value)
		//	}

		// Compression
		case "compress":
			return errors.New("compression not implemented yet")

		// Enable client side placeholder substitution
		case "interpolateParams":
			var isBool bool
			cfg.InterpolateParams, isBool = parseBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = parseBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// I/O read Timeout
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// Reject read-only connections
		case "rejectReadOnly":
			var isBool bool
			cfg.RejectReadOnly, isBool = parseBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Server public key
		case "serverPubKey":
			name, err := url.QueryUnescape(value)
			if err != nil {
				return fmt.Errorf("invalid value for server pub key name: %v", err)
			}
			cfg.ServerPubKey = name

		// Strict mode
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/mysql/wiki/strict-mode")

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			boolValue, isBool := parseBool(value)
			if isBool {
				if boolValue {
					cfg.TLSConfig = "true"
				} else {
					cfg.TLSConfig = "false"
				}
			} else if vl := strings.ToLower(value); vl == "skip-verify" || vl == "preferred" {
				cfg.TLSConfig = vl
			} else {
				name, err := url.QueryUnescape(value)
				if err != nil {
					return fmt.Errorf("invalid value for TLS config name: %v", err)
				}
				cfg.TLSConfig = name
			}

		// I/O write Timeout
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(value)
			if err != nil {
				return
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

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, DEFAULT_ADDR_PORT)
	}
	return addr
}
