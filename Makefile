build:
	@go build .

deps:
	@godep restore

test: build
	@go fmt .
	@go vet .
	@go test -v -race .
