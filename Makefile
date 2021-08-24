dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: ci
ci: deps checkgofmt vet staticcheck ineffassign predeclared test

.PHONY: deps
deps:
	cd bigtable && go get -d -v -t ./...
	cd storage && go get -d -v -t ./...

.PHONY: updatedeps
updatedeps:
	cd bigtable && go get -d -v -t -u -f ./...
	cd storage && go get -d -v -t -u -f ./...

.PHONY: install
install:
	cd bigtable && go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...
	cd storage && go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...

.PHONY: release
release:
	@GO111MODULE=on go install github.com/goreleaser/goreleaser
	goreleaser --rm-dist

.PHONY: docker
docker:
	@echo $(dev_build_version) > VERSION
	docker build --target=gcsemulator -t fullstorydev/gcsemulator:$(dev_build_version)
	docker build --target=cbtemulator -t fullstorydev/cbtemulator:$(dev_build_version)
	@rm VERSION

.PHONY: checkgofmt
checkgofmt:
	cd bigtable && gofmt -s -l .
	@if [ -n "$$(cd bigtable && gofmt -s -l .)" ]; then \
		exit 1; \
	fi

	cd storage && gofmt -s -l .
	@if [ -n "$$(cd storage && gofmt -s -l .)" ]; then \
		exit 1; \
	fi

.PHONY: vet
vet:
	# won't run for bigtable since it's a fork and the source already contains some problems
	# cd bigtable && go vet ./...
	cd storage && go vet ./...

# This all works fine with Go modules, but without modules,
# CI is just getting latest master for dependencies like grpc.
.PHONY: staticcheck
staticcheck:
	@GO111MODULE=on go install honnef.co/go/tools/cmd/staticcheck
	cd bigtable && staticcheck ./...
	cd storage && staticcheck ./...

.PHONY: ineffassign
ineffassign:
	@GO111MODULE=on go install github.com/gordonklaus/ineffassign
	cd bigtable && ineffassign .
	cd storage && ineffassign .

.PHONY: predeclared
predeclared:
	@GO111MODULE=on go install github.com/nishanths/predeclared
	cd bigtable && predeclared .
	cd storage && predeclared .

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: golint
golint:
	@GO111MODULE=on go install golang.org/x/lint/golint
	cd bigtable && golint -min_confidence 0.9 -set_exit_status ./...
	cd storage && golint -min_confidence 0.9 -set_exit_status ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: errcheck
errcheck:
	@GO111MODULE=on go install github.com/kisielk/errcheck
	cd bigtable && errcheck ./...
	cd storage && errcheck ./...

.PHONY: test
test:
	cd bigtable && go test -race ./...
	cd storage && go test -race ./...
