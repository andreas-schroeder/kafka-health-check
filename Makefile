build:
	@go build .

install:
	@go install .

deps:
	@go get -u -v github.com/kardianos/govendor
	@govendor sync
	@cd compatibility; govendor sync

test: build
	@go fmt . ./check
	@go vet . ./check
	@go test -v  ./check
	@go test -race ./check

test-no-race: build
	@go fmt . ./check
	@go vet . ./check
	@go test -v  ./check

compatibility: build
	@go run compatibility/test.go -base-dir "./compatibility" -health-check "./kafka-health-check"
