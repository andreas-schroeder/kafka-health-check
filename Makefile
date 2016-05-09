build:
	@go build .

install:
	@go install .

deps:
	@go get -u github.com/kardianos/govendor
	@govendor sync

test: build
	@go fmt . ./check
	@go vet . ./check
	@go test -v -race ./check
