# Linux I/O Benchmark for Schedulers (iobs)
An I/O workload automation and metric analysis tool used to characterize different workloads for different configurations. The following Linux tools are utilized internally: `blktrace`, `blkparse`, `btt`, `blkrawverify`, and (optionally) `fio`.

## Installation
The latest version can be obtained via `wget`:
```bash
$ wget https://raw.githubusercontent.com/JaredLGillespie/iobs/master/iobs.py
```
## Getting Started
Executing the script without any arguments shows the different arguments that can be used:
```bash
$ sudo python3 iobs.py
iobs.py 0.3.1
Usage: iobs.py <file> [-c] [-l] [-o <output>] [-r <retry>] [-v] [-x]
Command Line Arguments:
<file>            : The configuration file to use.
-c                : (OPTIONAL) The application will continue in the case of a job failure.
-l                : (OPTIONAL) Logs debugging information to an iobs.log file.
-o <output>       : (OPTIONAL) Outputs metric information to a file.
-r <retry>        : (OPTIONAL) Used to retry a job more than once if failure occurs. Defaults to 1.
-v                : (OPTIONAL) Prints verbose information to the STDOUT.
-x                : (OPTIONAL) Attempts to clean up intermediate files.
```

### Input
The main input to the tool should be a file in [INI](https://en.wikipedia.org/wiki/INI_file) format which has the configurations for how to run each of the workloads. The possible configuration settings for each workload is the following:
* command [str]- The workload generation command to run (e.x. fio ...).
* delay [int] - The amount of delay between running the workload and starting the trace. Defaults to 0.
* device [str] - The device to run the trace on (e.x. /dev/sdd).
* schedulers [str] - The io schedulers to use (e.x. noop, cfq, deadline, none, mq-deadline, bfq, kyber).
* repetition [int] - The number of times to repeat the workloads (will aggregate and average metrics). Defaults to 0.
* runtime [int] - The amount of time in seconds to run the trace (should match the workload runtime).
* workload [str] - The name of the workload generation tool (e.x. fio).

Each of these commands should be under a header [...] indicating a specific job. The global header [global] can be used for configuring settings that are the same for all jobs.

Example input files can be found under the [examples](https://github.com/JaredLGillespie/iobs/tree/master/examples) folder.

### Output
Upon completion of each job, the following metrics are output to STDOUT:
* Latency [µs]
* Submission Latency [µs]
* Completion Latency [µs]
* File System Latency [µs]
* Block Layer Latency [µs]
* Device Latency [µs]
* IOPS
* Throughput [1024 MB/s]
* Total IO [KB]

The following is a sample output the is given for a job:
```bash
(kyber) (/dev/nvme0n1):
Latency [µs]: (read): 5472.80 (write): 0.00
Submission Latency [µs]: (read): 519.02 (write): 0.00
Completion Latency [µs]: (read): 4952.97 (write): 0.00
File System Latency [µs]: (read): 2.35 (write): -4950.62
Block Layer Latency [µs]: 35.65
Device Latency [µs]: 4914.97
IOPS: (read) 182.51 (write) 0.00
Throughput [1024 MB/s]: (read) 912.57 (write) 0.00
Total IO [KB]: 560686080.00
```

In addition to STDOUT, an output file can be given via the `[-o] < output>` command-line argument. This file will be written to as a csv with the first row being a header of the following columns:
* device
* io-depth
* workload
* scheduler
* slat-read
* slat-write
* clat-read
* clat-write
* lat-read
* lat-write
* q2c
* d2c
* fslat-read
* fslat-write
* bslat
* iops-read
* iops-write
* throughput-read
* throughput-write
* io-kbytes
* start-time
* stop-time

## License
Copyright (c) 2018 UofL Computer Systems Lab. See [LICENSE](https://github.com/JaredLGillespie/iobs/blob/master/LICENSE) for details.
