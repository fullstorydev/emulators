dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: ci
ci:
	$(MAKE) -C bigtable ci
	$(MAKE) -C storage ci

.PHONY: install
install:
	$(MAKE) -C bigtable install
	$(MAKE) -C storage install

.PHONY: docker
docker:
	$(MAKE) -C bigtable docker
	$(MAKE) -C storage docker

.PHONY: test
test:
	$(MAKE) -C bigtable test
	$(MAKE) -C storage test
