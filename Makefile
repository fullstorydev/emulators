dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: ci-all
ci-all: ci-bigtable ci-storage

.PHONY: ci-bigtable
ci-bigtable: bigtable
	SERVICE=bigtable make deps
	SERVICE=bigtable make checkgofmt
	# SERVICE=bigtable make vet
	SERVICE=bigtable make staticcheck
	SERVICE=bigtable make ineffassign
	SERVICE=bigtable make predeclared
	SERVICE=bigtable make test

.PHONY: ci-storage
ci-storage: storage
	SERVICE=storage make deps
	SERVICE=storage make checkgofmt
	SERVICE=storage make vet
	SERVICE=storage make staticcheck
	SERVICE=storage make ineffassign
	SERVICE=storage make predeclared
	SERVICE=storage make test

.PHONY: bigtable
bigtable:
	$(eval SERVICE := bigtable)

.PHONY: storage
storage:
	$(eval SERVICE := bigtable)

.PHONY: deps
deps:
	cd ${SERVICE} && go get -d -v -t ./...

.PHONY: updatedeps
updatedeps:
	cd ${SERVICE} && go get -d -v -t -u -f ./...

.PHONY: install
install:
	cd ${SERVICE} && go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...

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
	cd ${SERVICE} && gofmt -s -l .
	@if [ -n "$$(cd ${SERVICE} && gofmt -s -l .)" ]; then \
		exit 1; \
	fi

.PHONY: vet
vet:
	cd ${SERVICE} && go vet ./...

# This all works fine with Go modules, but without modules,
# CI is just getting latest master for dependencies like grpc.
.PHONY: staticcheck
staticcheck:
	@GO111MODULE=on go install honnef.co/go/tools/cmd/staticcheck
	cd ${SERVICE} && staticcheck ./...

.PHONY: ineffassign
ineffassign:
	@GO111MODULE=on go install github.com/gordonklaus/ineffassign
	cd ${SERVICE} && ineffassign .

.PHONY: predeclared
predeclared:
	@GO111MODULE=on go install github.com/nishanths/predeclared
	cd ${SERVICE} && predeclared ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: golint
golint:
	@GO111MODULE=on go install golang.org/x/lint/golint
	cd ${SERVICE} && golint -min_confidence 0.9 -set_exit_status ./...

# Intentionally omitted from CI, but target here for ad-hoc reports.
.PHONY: errcheck
errcheck:
	@GO111MODULE=on go install github.com/kisielk/errcheck
	cd ${SERVICE} && errcheck ./...

.PHONY: test
test:
	cd ${SERVICE} && go test -race ./...
