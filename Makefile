dev_build_version=$(shell git describe --tags --always --dirty)

# TODO: run golint and errcheck, but only to catch *new* violations and
# decide whether to change code or not (e.g. we need to be able to whitelist
# violations already in the code). They can be useful to catch errors, but
# they are just too noisy to be a requirement for a CI -- we don't even *want*
# to fix some of the things they consider to be violations.
.PHONY: ci
ci: deps checkgofmt vet staticcheck ineffassign predeclared test

.PHONY: bigtable
bigtable:
	$(eval SERVICE := bigtable)

.PHONY: storage
storage:
	$(eval SERVICE := storage)

.PHONY: deps
deps:
	cd ${SERVICE} && go get -d -v -t ./...

.PHONY: updatedeps
updatedeps:
	cd ${SERVICE} && go get -d -v -t -u -f ./...

.PHONY: install
install:
	go install -ldflags '-X "main.version=dev build $(dev_build_version)"' ./...

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
	cd ${SERVICE} && predeclared .

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
