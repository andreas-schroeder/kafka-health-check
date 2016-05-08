build:
	@go build .

install:
	@go install .

deps:
	@godep restore

test: build
	@go fmt .
	@go vet .
	@go test -v -race .
