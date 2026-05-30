PROTOC ?= C:/msys64/mingw64/bin/protoc.exe

.PHONY: proto build test bench docker-up docker-down

proto:
	$(PROTOC) --proto_path=proto \
		--go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		proto/burrow.proto

build:
	go build -o bin/burrow-broker.exe ./cmd/broker
	go build -o bin/burrow-cli.exe ./cmd/cli

test:
	go test -race -timeout 120s ./...

bench:
	go test -run='^$$' -bench=. -benchmem -benchtime=10s ./bench/...

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v
