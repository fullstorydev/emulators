dev_build_version=$(shell git describe --tags --always --dirty)

.PHONY: docker
docker:
	@echo $(dev_build_version) > VERSION
	docker build --target=gcsemulator -t fullstorydev/gcsemulator:$(dev_build_version) -t fullstorydev/gcsemulator:latest .
	docker build --target=cbtemulator -t fullstorydev/cbtemulator:$(dev_build_version) -t fullstorydev/cbtemulator:latest .
	@rm VERSION
