module github.com/fullstorydev/emulators/examples

go 1.11

require (
	cloud.google.com/go/bigtable v1.10.1
	github.com/fullstorydev/emulators/bigtable v0.0.0
	github.com/fullstorydev/emulators/storage v0.0.0
	github.com/testcontainers/testcontainers-go v0.11.1
	google.golang.org/api v0.54.0
	google.golang.org/grpc v1.40.0
)

replace (
	github.com/fullstorydev/emulators/bigtable => ../bigtable
	github.com/fullstorydev/emulators/storage => ../storage
)
