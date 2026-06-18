# gossip-glomers
This repo implements the [fly.io distributed systems challenges](https://fly.io/dist-sys/) challenges, which consist of the following [maelstrom test workloads](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md):
* Echo
* Unique-ids
* Broadcast
* G-counter / pn-counter
* Kafka
* Txn-rw-register (buggy implementation done; abandoned in favor of txn-list-append)

It also contains the following additional workloads:
* G-set
* Txn-list-append (in-progress)
* Lin-kv (TODO)

## go project setup
```bash
mkdir $PROJECT_NAME
cd $PROJECT_NAME
go mod init $PROJECT_NAME
go mod tidy
go get github.com/jepsen-io/maelstrom/demo/go
go install .
```
