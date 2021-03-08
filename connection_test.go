package mdb

import (
	"database/sql/driver"
	"gitlab.com/blockpoint/utilities/odbc/golang/protocolBuffers/odbc"
	"testing"
)

func TestConn_interpolateParams(t *testing.T) {
	type fields struct {
		cfg                 *Config
		status              statusFlag
		MDBServiceClient    odbc.MDBServiceClient
		activeQuery         bool
		queryResponseStream *odbc.MDBService_QueryClient
	}
	type args struct {
		query string
		args  []driver.Value
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
			fields:   fields{
				cfg: &Config{
					InterpolateParams: true,
					MaxAllowedPacket: 10000,
				},
				MDBServiceClient:    nil,
				activeQuery:         false,
				queryResponseStream: nil,
			},
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
		// TODO: Add test cases.
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
