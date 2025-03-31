# dev_build_prefixed_version has the format "storage/<dev_version>"
dev_build_prefixed_version=$(shell git describe --tags --always --match 'storage/*' --dirty)
# dev_build_version is like dev_build_prefixed_version, but without the "storage/" prefix
dev_build_version=$(shell echo $(dev_build_prefixed_version) | sed 's/^storage\///')

.PHONY: ci
ci: deps checkgofmt vet staticcheck ineffassign predeclared golint errcheck test

.PHONY: deps
deps:
	go mod download

.PHONY: updatedeps
updatedeps:
	go get -d -v -t -u -f ./...
	go mod tidy

.PHONY: checkgofmt
checkgofmt:
	gofmt -s -l .
	@if [ -n "$$(gofmt -s -l .)" ]; then \
		exit 1; \
	fi

.PHONY: vet
vet:
	go vet ./...

.PHONY: staticcheck
staticcheck:
	@go install honnef.co/go/tools/cmd/staticcheck@v0.6.0
	staticcheck ./...

.PHONY: ineffassign
ineffassign:
	@go install github.com/gordonklaus/ineffassign@7953dde2c7bf
	ineffassign .

.PHONY: predeclared
predeclared:
	@go install github.com/dragonsinth/predeclared@154d704ce28e2a93072c21de8f4fa13392b42689
	predeclared ./...

.PHONY: golint
golint:
	@go install golang.org/x/lint/golint@v0.0.0-20210508222113-6edffad5e616
	golint -min_confidence 0.9 -set_exit_status ./...

.PHONY: errcheck
errcheck:
	@go install github.com/kisielk/errcheck@v1.2.0
	errcheck ./...

.PHONY: test
test: deps
	go test -race ./...

.PHONY: install
install:
	go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...

.PHONY: release
release:
	@go install github.com/goreleaser/goreleaser@v1.21.0
	goreleaser release --clean

.PHONY: docker
docker:
	@echo $(dev_build_version) > VERSION
	docker build -t fullstorydev/gcsemulator:$(dev_build_version) .
	@rm VERSION
