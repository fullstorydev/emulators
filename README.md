# emulators
High quality cloud service emulators for local development stacks

## Why?

At FullStory, our entire product and backend software stack runs in each engineer's local workstation.  This high-quality local development experience keeps our engineers happy and productive, because they are able to build and test features or reproduce and fix bugs quickly and easily:

- All of our own backend services are designed to operate correctly in a local environment.
- Open source, third party services such as Redis, Zookeeper, or Solr can be easily configured to run locally.
- Google Cloud infrastructure must be emulated.

## What Google Cloud services do we emulate?

| Service                    | Persistence? | Status                  | Notes                                                                                                                         |
|----------------------------|--------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Google Bigtable            | Yes          | Shipped, see below      | Fork of [bigtable/bttest](https://github.com/googleapis/google-cloud-go/tree/master/bigtable/bttest)                          |
| Google Cloud Storage (GCS) | Yes          | Coming soon             | Written from scratch                                                                                                          |
| Google Pubsub              | No           | Considering persistence | Vanilla [pubsub/pstest](https://github.com/googleapis/google-cloud-go/tree/master/pubsub/pstest)                              |
| Google Cloud Functions     | n/a          | In consideration        | Thin wrapper that manages `node` processes.                                                                                   |
| Google Datastore           | Yes          | -                       | Google's [Datastore emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator) (written in Java) works great |

## Google Bigtable Emulator

(TODO)

## Google Cloud Storage Emulator

Coming soon.
