# Notes
Goal:
* Messages-per-operation < 20
* Median latency < 1 second
* Maximum latency < 2 seconds

## TODO
* Consider adding leader election algorithm to 3d and 3e.

## Results
### Single leader node
Pick one node to be root of tree.

No partition:
```
:net {:all {:send-count 18128,
            :recv-count 18128,
            :msg-count 18128,
            :msgs-per-op 10.370709},
       :clients {:send-count 3596, :recv-count 3596, :msg-count 3596},
       :servers {:send-count 14532,
                 :recv-count 14532,
                 :msg-count 14532,
                 :msgs-per-op 8.313501},
       :valid? true}
:stable-latencies {0 0,
                   0.5 230,
                   0.95 309,
                   0.99 346,
                   1 397}
```
With partition:
```
:net {:all {:send-count 17123,
            :recv-count 15190,
            :msg-count 17123,
            :msgs-per-op 9.674011},
       :clients {:send-count 3640, :recv-count 3640, :msg-count 3640},
       :servers {:send-count 13483,
                 :recv-count 11550,
                 :msg-count 13483,
                 :msgs-per-op 7.617514},
       :valid? true}
:stable-latencies {0 0,
                   0.5 1166,
                   0.95 9035,
                   0.99 9874,
                   1 10136},
```
As expected, this performs very badly under network partition when the leader is down.

### Decentralized broadcast
Each node is root of its own tree; broadcasts to every node then stops. Implemented in `alternate_impl.go`.

No partition:
```
:net {:all {:send-count 21450,
            :recv-count 21450,
            :msg-count 21450,
            :msgs-per-op 12.625073},
       :clients {:send-count 3498, :recv-count 3498, :msg-count 3498},
       :servers {:send-count 17952,
                 :recv-count 17952,
                 :msg-count 17952,
                 :msgs-per-op 10.5662155},
       :valid? true}
:stable-latencies {0 0,
                   0.5 577,
                   0.95 991,
                   0.99 998,
                   1 1047},
```

With partition:
```
:net {:all {:send-count 21516,
            :recv-count 21372,
            :msg-count 21516,
            :msgs-per-op 12.59719},
       :clients {:send-count 3516, :recv-count 3516, :msg-count 3516},
       :servers {:send-count 18000,
                 :recv-count 17856,
                 :msg-count 18000,
                 :msgs-per-op 10.538642},
       :valid? true}
:stable-latencies {0 0,
                   0.5 637,
                   0.95 2436,
                   0.99 3355,
                   1 3507},
```

As expected, this is less efficient and slower when there are no partitions (since we lose the efficiency of the leader node
queueing up the requests), but faster when there is a network partition, since there is no leader bottleneck.
