# Best-effort Broadcast by all processsors

This repository contains a reference implementation of Multi-sender best-effort broadcast analysed in the [pipes framework paper](https://eprint.iacr.org/2025/1116.pdf). As discussed in the paper, we make simplifying changes to the implementation. 


In particular, we upload the entire block of size $\frac{\alpha M}{1-\alpha}$ at once and then wait for an estimated clearing time (provided as an
input parameter) before transmitting metadata. If this estimate is too small, metadata may be sent before previous transmissions complete, increasing latency. A separate estimate is also needed for metadata upload time. Consequently, we experiment with multiple values for these estimates to identify those that yield latency close to the theoretical predictions for the modified protocol.


## Quick Start

The protocol is implemented in Rust, while benchmarking scripts are written in Python and executed using [Fabric](http://www.fabfile.org/). 

To deploy and benchmark a 15-node local testbed, clone the repository and install the Python dependencies:

```
$ git clone https://github.com/nibeshrestha/hydrangea.git
$ git checkout beb-all-sender
$ cd pipes/benchmark
$ pip install -r requirements.txt
```
You will also need Clang (required for RocksDB) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux)
 (used to run all nodes and clients in the background). Once installed, you can launch a local benchmark using Fabric:

```
$ fab local
```

The first execution may take longer, as it compiles the Rust code in `release` mode. You can customize various benchmarking parameters in fabfile.py before running. Once the benchmark completes, a summary of the execution will be printed, similar to the example shown below.

```
-----------------------------------------
 SUMMARY:
-----------------------------------------
Logs generated at: 2025-12-04 16:18:20.302969

 + CONFIG:
 Consensus run in isolation
 Leader elector: Simple
 Faults: 0 node(s)
 Committee size: 15 node(s)
 F: 2
 C: 2
 K: 4

 Block size: 10 Certificates
 Timeout delay: 100 ms
 Sync retry delay: 5,000 ms
 Sync retry nodes: 3 node(s)

 + RESULTS:
 Execution time: 20 s
 Block Proposal time : 13 ms

 Block Commit:
   To First Commit:
     Mean Latency: 11 ms
     Median Latency: 11 ms
     BLPS: 76 blocks/s
   To Last Commit:
     Mean Latency: 16 ms
     Median Latency: 15 ms
     BLPS: 76 blocks/s
   Total Blocks Committed: 1,535
-----------------------------------------
```

## License

This software is licensed as [Apache 2.0](LICENSE).