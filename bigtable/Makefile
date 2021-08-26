dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: ci
ci: deps checkgofmt vet staticcheck ineffassign predeclared test

.PHONY: deps
deps:
	$(MAKE) -C ../tools
	go get -d -v -t ./...

.PHONY: updatedeps
updatedeps:
	go get -d -v -t -u -f ./...

.PHONY: install
install:
	go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...

.PHONY: release
release:
	goreleaser --rm-dist

.PHONY: docker
docker:
	@echo $(dev_build_version) > VERSION
	docker build --target=cbtemulator -t fullstorydev/cbtemulator:$(dev_build_version)
	@rm VERSION

.PHONY: checkgofmt
checkgofmt:
	gofmt -s -l .
	@if [ -n "$$(gofmt -s -l .)" ]; then \
		exit 1; \
	fi

.PHONY: vet
vet:
	go vet -composites=false ./...

# This all works fine with Go modules, but without modules,
# CI is just getting latest master for dependencies like grpc.
.PHONY: staticcheck
staticcheck:
	staticcheck ./...

.PHONY: ineffassign
ineffassign:
	ineffassign ./...

.PHONY: predeclared
predeclared:
	predeclared ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: golint
golint:
	golint -min_confidence 0.9 -set_exit_status ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: errcheck
errcheck:
	errcheck ./...

.PHONY: test
test:
	go test -race ./...
