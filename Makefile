build:
	@go build .

install:
	@go install .

deps:
	@go get -u github.com/kardianos/govendor
	@govendor sync
	cd compatibility; govendor sync

test: build
	@go fmt . ./check
	@go vet . ./check
	@go test -v -race ./check

compatibility: build
	@go run compatibility/test.go -base-dir "./compatibility" -health-check "./kafka-health-check"
