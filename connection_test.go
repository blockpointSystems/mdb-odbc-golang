package mdb

import (
	"database/sql/driver"
	"encoding/base32"
	"fmt"
	"gitlab.com/blockpoint/mdb-redesign/common/cryptography"
	"gitlab.com/blockpoint/protocol-buffers/v1/odbc"

	"testing"
)

func TestConn_interpolateParams(t *testing.T) {
	type fields struct {
		cfg                 *Config
		status              statusFlag
		MDBServiceClient    odbc.MDBServiceClient
		activeQuery         uint32
		queryResponseStream *odbc.MDBService_QueryClient
	}
	type args struct {
		query string
		args  []driver.Value
	}

	baseFields := fields{
		cfg: &Config{
			InterpolateParams: true,
			MaxAllowedPacket:  10000,
		},
		MDBServiceClient:    nil,
		activeQuery:         0,
		queryResponseStream: nil,
	}

	tmpPWHash, tmpSalt := cryptography.HashPassword("meow")
	expOutput := "INSERT main.user (stripe_id, first_name, last_name, firm_name, email, phone_number, address1, address2, city, state, zip, password_hash, salt) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", %d, \"%s\", %d, %d) OUTPUT id"

	var secret, err = base32.StdEncoding.DecodeString("")
	if err != nil {
		panic(err)
	}

	tests := []struct {
		name     string
		fields   fields
		args     args
		wantResp string
		wantErr  bool
	}{
		{
			name:     "Basic test 1",
			fields:   baseFields,
			args:     args{
				query: "SELECT * FROM main.user WHERE user.name <> ? AND user.age > ?",
				args:  []driver.Value{
					"john",
					int64(21),
				},
			},
			wantResp: "SELECT * FROM main.user WHERE user.name <> \"john\" AND user.age > 21",
			wantErr:  false,
		},
		{
			name:     "Basic test 2",
			fields:   baseFields,
			args:     args{
				query: "DISCONTINUE main.user (id) VALUES (?)",
				args:  []driver.Value{
					uint64(9827341),
				},
			},
			wantResp: "DISCONTINUE main.user (id) VALUES (9827341)",
			wantErr:  false,
		},
		{
			name:     "Basic test 3",
			fields:   baseFields,
			args:     args{
				query: "DISCONTINUE main.user (id) VALUES (?)",
				args:  []driver.Value{
					[]byte{},
				},
			},
			wantResp: "DISCONTINUE main.user (id) VALUES ([])",
			wantErr:  false,
		},
		{
			name:     "Basic test 4",
			fields:   baseFields,
			args:     args{
				query: "INSERT main.user (stripe_id, first_name, last_name, firm_name, email, phone_number, address1, address2, city, state, zip, password_hash, salt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) OUTPUT id",
				args:  []driver.Value{
					"J3V60cVnmGcvM3",
					"John", "Smith", "Wealth Alliance",
					"john@smith.com", "+1 (858) 994-3112",
					"PO Box 4112", "", "San Diego", int64(2), "92067",
					tmpPWHash, tmpSalt,
				},
			},
			wantResp: fmt.Sprintf(
				expOutput,
				"J3V60cVnmGcvM3",
				"John", "Smith", "Wealth Alliance",
				"john@smith.com", "+1 (858) 994-3112",
				"PO Box 4112", "", "San Diego", 2, "92067",
				tmpPWHash, tmpSalt,
			),
			wantErr:  false,
		},
		{
			name:     "Basic test 5",
			fields:   baseFields,
			args:     args{
				query: "AMEND user (id, two_factor_secret) VALUES (?, ?)",
				args:  []driver.Value{
					int64(312), secret,
				},
			},
			wantResp: "DISCONTINUE main.user (id) VALUES (312, [])",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &Conn{
				cfg:                 tt.fields.cfg,
				status:              tt.fields.status,
				MDBServiceClient:    tt.fields.MDBServiceClient,
				activeQuery:         tt.fields.activeQuery,
				queryResponseStream: tt.fields.queryResponseStream,
			}
			gotResp, err := db.interpolateParams(tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("interpolateParams() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResp != tt.wantResp {
				t.Errorf("interpolateParams() gotResp = %v, want %v", gotResp, tt.wantResp)
			}
		})
	}
}
