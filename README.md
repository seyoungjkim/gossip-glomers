# gossip-glomers
https://fly.io/dist-sys/ challenges

## go project setup
```go
mkdir <project-name>
go mod init <project-name>
go mod tidy
go get github.com/jepsen-io/maelstrom/demo/go
go install .
```

## TODO
* Consider adding leader election algorithm to 3d and 3e
