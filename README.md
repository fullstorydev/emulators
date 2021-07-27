# emulators
High quality Google Cloud service emulators for local development stacks

## Why?

At FullStory, our entire product and backend software stack runs in each engineer's local workstation. This high-quality local development experience keeps our engineers happy and productive, because they are able to:

- build and test features locally
- reproduce and fix bugs quickly and easily
- run high-quality services in unit and integration tests

Our local development and testing story is simpler when our live code can rely on expected services to exist, and we don't have to write as many mocks.

Some of the ways we achieve this:

- Our own backend services operate in a reasonable manner in a local environment.
- Open source, third party services (such as Redis, Zookeeper, or Solr) run locally.
- We emulate Google Cloud infrastructure.

## What Google Cloud services do we emulate?

| Service                    | Persistence? | Status                  | Notes                                                                                                                         |
|----------------------------|--------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Google Bigtable            | Yes          | Shipped, see below      | Fork of [bigtable/bttest](https://github.com/googleapis/google-cloud-go/tree/master/bigtable/bttest)                          |
| Google Cloud Storage (GCS) | Yes          | Shipped, see below      | Written from scratch                                                                                                          |
| Google Pubsub              | No           | Considering persistence | Vanilla [pubsub/pstest](https://github.com/googleapis/google-cloud-go/tree/master/pubsub/pstest)                              |
| Google Cloud Functions     | n/a          | In consideration        | Thin wrapper that manages `node` processes.                                                                                   |
| Google Datastore           | Yes          | -                       | Google's [Datastore emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator) (written in Java) works great |

## Google Bigtable Emulator

Our bigtable emulator is a fork of [bigtable/bttest](https://github.com/googleapis/google-cloud-go/tree/master/bigtable/bttest).  A summary of the changes we made:
- The core operates directly on Bigtable protobuf types, such as Table and Row, instead of bespoke types.
- The storage layer is pluggable and operates on protos.
- Leveldb is the default storage implementation, and runs either in-memory (transient for unit tests) or on disk (long running, persistence).

### Installing

```sh
go get github.com/fullstorydev/emulators/bigtable
go install github.com/fullstorydev/emulators/bigtable/... # for the command-line `cbtemulator`
```

### Running, out of process

Example, running on a specific port, with persistence:
```sh
> cbtemulator -port 8888 -dir var/bigtable
Writing to: var/bigtable
Cloud Bigtable emulator running on 127.0.0.1:8888
```

Usage:
```
  -dir string
    	if set, use persistence in the given directory
  -host string
    	the address to bind to on the local machine (default "localhost")
  -port int
    	the port number to bind to on the local machine (default 9000)
```

### Running, in process

You can link bigtable emulator into existing Go binaries as a drop-in replacement for `bigtable/bttest`.

For unit tests:
```go
	// start an in-memory leveldb BigTable test server (for unit tests)
	svr, err := bttest.NewServer("127.0.0.1:0", grpc.MaxRecvMsgSize(math.MaxInt32))
	if err != nil { 
		// ...
	}
	// bigtable.NewClient (via DefaultClientOptions) will look at this env var to figure out what host to talk to
	os.Setenv("BIGTABLE_EMULATOR_HOST", svr.Addr)
```

For on-disk pesistence:
```go
	// start an leveldb-backed BigTable test server
	srv, err := bttest.NewServerWithOptions(fmt.Sprintf("127.0.0.1:%d", *btport), bttest.Options{
		Storage: bttest.LeveldbDiskStorage{
			Root: bigtableStorageDir,
			ErrLog: func(err error, msg string) {
				// wire into logging
			},
		},
		GrpcOpts: []grpc.ServerOption{grpc.MaxRecvMsgSize(maxGrpcMessageSize)},
	})
```

## Google Cloud Storage Emulator

### Running, out of process

Example, running on a specific port, with persistence:
```sh
> gcsemulator -port 8888 -dir var/storage
Writing to: var/storage
Cloud Storage emulator running on http://127.0.0.1:8888
```

Usage:
```
  -dir string
    	if set, use persistence in the given directory
  -host string
    	the address to bind to on the local machine (default "localhost")
  -port int
    	the port number to bind to on the local machine (default 9000)
```

For unit tests:
```go
	// TODO
```

For on-disk pesistence:
```go
	// TODO
```
