# dev_build_prefixed_version has the format "bigtable/<dev_version>"
dev_build_prefixed_version=$(shell git describe --tags --always --match 'bigtable/*' --dirty)
# dev_build_version is like dev_build_prefixed_version, but without the "bigtable/" prefix
dev_build_version=$(shell echo $(dev_build_prefixed_version) | sed 's/^bigtable\///')

.PHONY: ci
ci: deps checkgofmt vet staticcheck ineffassign predeclared golint errcheck test

.PHONY: deps
deps:
	go get -v -t ./...
	go mod tidy

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
	@go install github.com/nishanths/predeclared@245576f9a85c96ea16c750df3887f1d827f01e9c
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
	docker build -t fullstorydev/cbtemulator:$(dev_build_version) .
	@rm VERSION
