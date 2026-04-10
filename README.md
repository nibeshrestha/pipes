# Best-effort Broadcast by all processsors


This repository provides a reference implementation of multi-sender best-effort broadcast analyzed in the [Pipes framework paper](https://eprint.iacr.org/2025/1116.pdf). As described in the paper, we introduce several simplifying modifications compared to the original design, which transmits $S$ parcels per timeslot. Specifically, we instead upload the entire block of size $\frac{\alpha M}{1-\alpha}$ in a single step, and then pause for an estimated clearing time (given as an input parameter) before sending the metadata.

If the clearing-time estimate is too small, metadata may be transmitted before prior data transfers have completed, leading to increased latency. Additionally, a separate estimate is required for the metadata upload duration. To account for these factors, we evaluate multiple choices for these parameters and identify configurations that achieve latency close to the theoretical predictions of the modified protocol.


## Quick Start

The protocol is implemented in Rust, while benchmarking scripts are written in Python and executed using [Fabric](http://www.fabfile.org/). 

To deploy and benchmark a 15-node local testbed, clone the repository and install the Python dependencies:

```
$ git clone https://github.com/nibeshrestha/pipes.git
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
Logs generated at: 2026-04-09 19:56:58.150895

 + CONFIG:
 Consensus run in isolation
 Faults: 0 node(s)
 Committee size: 10 node(s)

 Block size: 3,061 B
 Dependent Meta size: 25,000 B
 Independent Meta size: 12,500 B
 Client Rate: 250,000.000000 
 Meta Prop Time: 100 ms
 Block Prop Time: 200 ms

 + RESULTS:
 Execution time: 29 s

 Block Delivered:
   Mean Latency: 14 ms
   Median Latency: 10 ms

 Meta Delivered:
   Mean Latency: 317 ms
   Median Latency: 313 ms
   Max Latency: 421 ms
   BLPS: 29 blocks/s
   Total Blocks Received: 831
-----------------------------------------
```

## Remote Run

The scripts are configured to deploy on GCP instances. Ensure that your GCP API credentials are placed in `benchmark/benchmark/key.json`. You can then install the required dependencies on the remote servers by running `fab install`.

We use the Linux tc command to introduce network delays and enforce bandwidth limits. To configure these settings, run `fab set-filter`. By default, the network delay (100 ms) and bandwidth (100 Mbit/s) are hardcoded in remote.py; you can modify these values in that file as needed.

Once the setup is complete, launch the experiment on the remote machines using `fab remote`.

## License

This software is licensed as [Apache 2.0](LICENSE).