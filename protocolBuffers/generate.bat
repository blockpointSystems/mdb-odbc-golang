:: The statements below generate the gRPC and protocol buffers for the bSQL packets

:: This statement generates the packets for the golang API
:: protoc master_pb/master.proto --go_out=plugins=grpc:$env:GOPATH\src

protoc odbc.proto --go_out=:%GOPATH%\src --go-grpc_out=:%GOPATH%\src
:: protoc odbc.proto --go_out=plugins=grpc:%GOPATH%\src