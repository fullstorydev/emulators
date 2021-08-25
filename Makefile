dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: ci
ci:
	cd bigtable && make ci
	cd storage && make ci

.PHONY: install
install:
	cd bigtable && make install
	cd storage && make install

.PHONY: release
release:
	@GO111MODULE=on go install github.com/goreleaser/goreleaser
	goreleaser --rm-dist

.PHONY: docker
docker:
	cd bigtable && make docker
	cd storage && make docker

.PHONY: test
test:
	cd bigtable && make test
	cd storage && make test
