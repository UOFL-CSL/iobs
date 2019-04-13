# Linux I/O Benchmark for Schedulers (iobs)

An I/O workload automation and metric analysis tool used to gauge the performance of a device. It provides a means of 
automating commonly run workloads with tools such as `fio` and `filebench`.

## Why Should I Use This?

The goal of `iobs` is to decrease the amount of manual work involved in running I/O experiments on devices. 

Commonly used tools for running workloads, such as `fio` and `filebench` are well suited for providing a means of 
benchmarking a device. However, when utilizing other tools or running multiple workloads with slight variations, the 
number of configuration  changes and formatting differences between tools makes for an inefficient amount of manual work
required to consolidate the results.

The biggest advantage of `iobs` is the reduction in work required to run many experiments with different combinations
of configurations.

## Installation

The latest version can be obtained from the [releases](https://github.com/uofl-csl/iobs/releases). 

The following steps are recommended in retrieving the package:
 1. Use `wget` to pull the latest [tarball](https://github.com/uofl-csl/iobs/releases) onto the machine. 
 2. Run `tar xvzf <file-name>.tar.gz` on the tarball to extract it.
 3. `cd` into the directory.
 4. Run `python setup.py install` to install the package and any dependencies.

## Getting Started

Executing the script with the `-h` flag shows the different arguments that can be used:
```bash
$ iobs -h
usage: iobs [-h] [--version] {execute, validate}

positional arguments:
  {execute, validate}

optional arguments:
  -h, --help  show this help message and exit
  --version   show program's version number and exit
```

### `iobs execute`

Executes one or more `iobs` configuration files.

```bash
$ iobs execute -h
usage: iobs execute [-h] [-o OUTPUT_DIRECTORY] [-l LOG_FILE]
                    [--log-level {1,2,3}] [-s] [-r RETRY_COUNT] [-c] [-d]
                    input [input ...]

positional arguments:
  input                 The configuration files to execute.

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_DIRECTORY, --output-directory OUTPUT_DIRECTORY
                        The output directory for output files. Defaults to the
                        current working directory.
  -l LOG_FILE, --log-file LOG_FILE
                        The file to log information to.
  --log-level {1,2,3}   The level of information to which to log: 1 (Debug), 2
                        (Info), 3 (Error). Defaults to 2.
  -s, --silent          Silences output to STDOUT.
  -r RETRY_COUNT, --retry-count RETRY_COUNT
                        Number of times to retry a failed workload. Defaults
                        to 1.
  -c, --continue-on-failure
                        If a input fails, continues executing other inputs;
                        otherwise exits the program.
  -d, --reset-device    Resets the device to original settings after
                        execution of each input.
```

### `iobs validate`

Validates one or more `iobs` configuration files.

```bash
$ iobs validate -h
usage: iobs validate [-h] input [input ...]

positional arguments:
  input                 The configuration files to validate.
```

## Configuration Files

Configuration files should be in [INI](https://en.wikipedia.org/wiki/INI_file) format. Each section should be contained
in square brackets [...] with configuration settings below the section. Different section names are reserved for special
purposes; all others are considered a workload section. Depending on the `workload_type` given under the `global` 
section, additional settings may be permitted under a given section.

Settings which accept lists of values should be separated by a comma `,`.

### `global`

Global settings for each of the workloads.

 * `workload_type` (required) - The type of workloads to run: `fio`.
   * **ex:** `workload_type=fio`
 * `devices` (required) - The devices to execute on.
   * **ex:** `devices=/dev/nvme0n1/,/dev/nvme1n1`
   * *NOTE: Should be valid block devices.*
 * `schedulers` (required) - The schedulers to use on the device.
   * **ex:** `schedulers=none,kyber,bfq,mq-deadline`
   * *NOTE: Should be available for each device specified.*
 * `repetitions` (optional) - The number of times to repeat the workloads. Defaults to 1.
   * **ex:**: `repetitions=5`

### `output`

Output settings for what metrics to write in the output `.csv` files. The output file will have the same name
as the input configuration file and the extension replaced with `.csv`. Note that the last column in the output
file will always be `END`.

 * `format` (optional) - The metrics to write. Default and allowed format depend on `workload_type` (see below).
 * `append_template` (optional) - Whether to append the `template` combinations. Defaults to True.
   * **ex:** `append_template=1` 
   * *NOTE: This should be set to `False` if a custom format is given which includes `template` information. Otherwise
            duplicate columns will be added to the output.*
 * `append_environment` (optional) - Whether to append the `environment` combinations. Defaults to True.
   * **ex:** `append_environment=1`
   * *NOTE: This should be set to `False` if a custom format is given which includes `environment` information. Otherwise
            duplicate columns will be added to the output.*
 * `append_blktrace` (optional) - Whether to append the `blktrace` metrics. Defaults to True.
   * **ex:** `append_blktrace=1`
   * *NOTE: This will do nothing if `enable_blktrace` isn't set in the `workload`. This should be set to `False` if a 
            custom format is given which includes `blktrace` information. Otherwise duplicate columns will be added to 
            the output.*
 * `ignore_missing` (optional) - Whether to ignore missing metrics. Defaults to False.
   * **ex:** `ignore_missing=1`
   * *NOTE: Missing metrics will use "NONE" as their value.*

The `format` accepts a list of metric names which should be retrieved from the workload and written in the output files.
Each metric name can accept the full name or an abbreviated name (if there is one). Also, when using the `template` 
section, the names of the `template` settings can be specified as well. For example, if the template setting
`rw=randread,randwrite` is specified, then the `rw` name can be used in the `format` to write the configuration used
in the workload.

The following `format` metric names are used by any `workload_type`:
 * `workload` (or `w`) - The name of the workload.
 * `device` (or `d`) - The name of the device.
 * `scheduler` (or `s`) - The name of the scheduler.

The following `format` metric names are used if `enable_blktrace` is set in the `workload` section:
 * `d2c-avg` - The average d2c.
 * `d2c-min` - The minimum d2c.
 * `d2c-max` - The maximum d2c.
 * `d2c-n` - The number of IOs used in d2c.
 * `q2c-avg` - The average q2c.
 * `q2c-min` - The minimum q2c.
 * `q2c-max` - The maximum q2c.
 * `q2c-n` - The number of IOs used in q2c.
 
**`filebench`**
 * `include_flowops` (optional) - Whether to include flowops metrics. Defaults to False.
   * **ex:** `include_lat_percentiles=1`
   * *NOTE: Each of the flowop metrics are added with the format of `flowopname-identifier` where identifier is one of 
     the following: `total-ops`, `throughput-ops`, `throughput-mb`, `average-lat`, `min-lat`, `max-lat`.*
 
The following `format` metric names are used by the `filebench` `workload_type`:
 * `runtime` (or `run`) - The total runtime for the job in seconds.
 * `total-ops` (or `ops`) - The total number of operations performance.
 * `throughput-ops` (or `top`) - The average throughput in ops/s.
 * `read-throughput-ops` (or `rto`) - The average read throughput in ops/s.
 * `write-throughput-ops` (or `wto`) - The average write throughput in ops/s.
 * `throughput-mb` (or `tmb`) - The average throughput in MB/s.
 * `average-lat` (or `avl`) - The average latency in ms.
 * `flowops` - The flowops metrics.
   * *NOTE: If `include_flowops=1` is not set, this is ignored. The number of columns depends
   on the number of flowops by `filebench`.*
            
The default `format` used if none is given is the following:
 * `workload`
 * `device`
 * `scheduler`
 * `runtime`
 * `total-ops`
 * `throughput-ops`
 * `read-throughput-ops`
 * `write-throughput-ops`
 * `throughput-mb`
 * `average-lat`
 * `flowops`

**`fio`**
 * `include_lat_percentiles` (optional) - Whether to include lat percentile metrics. Defaults to False.
   * **ex:** `include_lat_percentiles=1`
   * *NOTE: The `fio` workload file should have `lat_percentiles=1` set.*
 * `include_clat_percentiles` (optional) - Whether to include clat percentile metrics. Defaults to False.
   * **ex:** `include_clat_percentiles=1`
   * *NOTE: The `fio` workload file should have `clat_percentiles=1` set (it is typically enabled by default).*
 
The following `format` metric names are used by the `fio` `workload_type`:
 * `job-runtime` (or `run`) - The total runtime for the job in ms.
 * `total-ios-read` (or `tir`) - The total number of IOs read.
 * `total-ios-write` (or `tiw`) - The total number of IOs written.
 * `io-kbytes-read` (or `ibr`) - The total KB read.
 * `io-kbytes-write` (or `ibw`) - The total KB written.
 * `bw-read` (or `bwr`) - The average read bandwidth (throughput) in KB/s.
 * `bw-write` (or `bww`) - The average write bandwidth (throughput) in KB/s.
 * `iops-read` (or `opr`) - The average read IO/s.
 * `iops-write` (or `ipw`) - The average write IO/s.
 * `lat-min-read` (or `lir`) - The minimum read latency in ns.
 * `lat-min-write` (or `liw`) - The minimum write latency in ns.
 * `lat-max-read` (or `lar`) - The maximum read latency in ns.
 * `lat-max-write` (or `law`) - The maximum write latency in ns.
 * `lat-mean-read` (or `lmr`) - The average read latency in ns.
 * `lat-mean-write` (or `lmw`) - The average write latency in ns.
 * `lat-stddev-read` (or `lsr`) - The standard deviation of the read latency in ns.
 * `lat-stddev-write` (or `lsw`) - The standard deviation of the write latency in ns.
 * `slat-min-read` (or `sir`) - The minimum read submission latency in ns.
 * `slat-min-write` (or `siw`) - The minimum write submission latency in ns.
 * `slat-max-read` (or `sar`) - The maximum read submission latency in ns.
 * `slat-max-write` (or `saw`) - The maximum write submission latency in ns.
 * `slat-mean-read` (or `smr`) - The average read submission latency in ns.
 * `slat-mean-write` (or `smw`) - The average write submission latency in ns.
 * `slat-stddev-read` (or `ssr`) - The standard deviation of the read submission latency in ns.
 * `slat-stddev-write` (or `ssw`) - The standard deviation of the write submission latency in ns.
 * `clat-min-read` (or `cir`) - The minimum read completion latency in ns.
 * `clat-min-write` (or `ciw`) - The minimum write completion latency in ns.
 * `clat-max-read` (or `car`) - The maximum read completion latency in ns.
 * `clat-max-write` (or `caw`) - The maximum write completion latency in ns.
 * `clat-mean-read` (or `cmr`) - The average read completion latency in ns.
 * `clat-mean-write` (or `cmw`) - The average write completion latency in ns.
 * `clat-stddev-read` (or `csr`) - The standard deviation of the read completion latency in ns.
 * `clat-stddev-writ` (or `csw`) - The standard deviation of the write completion latency in ns.
 * `clat-percentile-read` (or `cpr`) - The read completion latency percentiles in ns.
   * *NOTE: If `include_clat_percentile=1` is not set, this is ignored. The number of columns depends
   on the number of percentiles reported by `fio`.*
 * `clat-percentile-write` (or `cpw`) - The write completion latency percentiles in ns.
   * *NOTE: If `include_clat_percentile=1` is not set, this is ignored. The number of columns depends
   on the number of percentiles reported by `fio`.*
 * `lat-percentile-read` (or `lpr`) - The read latency percentiles in ns.
   * *NOTE: If `include_clat_percentile=1` is not set, this is ignored. The number of columns depends
   on the number of percentiles reported by `fio`.*
 * `lat-percentile-write` (or `lpw`) - The write latency percentiles in ns.
   * *NOTE: If `include_clat_percentile=1` is not set, this is ignored. The number of columns depends
   on the number of percentiles reported by `fio`.*
            
The default `format` used if none is given is the following:
 * `workload`
 * `device`
 * `scheduler`
 * `job-runtime`
 * `total-ios-read`
 * `total-ios-write`
 * `io-kbytes-read`
 * `io-kbytes-write`
 * `bw-read`
 * `bw-write`
 * `iops-read`
 * `iops-write`
 * `lat-min-read`
 * `lat-min-write`
 * `lat-max-read`
 * `lat-max-write`
 * `lat-mean-read`
 * `lat-mean-write`
 * `lat-stddev-read`
 * `lat-stddev-write`
 * `slat-min-read`
 * `slat-min-write`
 * `slat-max-read`
 * `slat-max-write`
 * `slat-mean-read`
 * `slat-mean-write`
 * `slat-stddev-read`
 * `slat-stddev-write`
 * `clat-min-read`
 * `clat-min-write`
 * `clat-max-read`
 * `clat-max-write`
 * `clat-mean-read`
 * `clat-mean-write`
 * `clat-stddev-read`
 * `clat-stddev-write`
 * `clat-percentile-read`
 * `clat-percentile-write`

### `template`

Template settings for interpolating different setting combinations into `workload` files.

 * `enabled` (optional) - Whether to enable templating. Defaults to False.
   * **ex:** `enabled=1`

All other settings added under this section are used to interpolate the `workload` files. When files are interpolated,
an interpolated copy is made with the name `__temp__` appended to them. To provide settings to interpolate within a 
`workload` file, the following syntax should be used: `<%setting-name%>`. By default, the following will always be 
interpolated if templating is enabled: `<%device%>` the device, `<%device_name%>` the name of the device (i.e. no /dev/),
`<%scheduler%>` the scheduler.

The following is an example `template` section:

```ini
[template]
enabled=1
rw=randread,randwrite
iodepth=1,2,4,8,16
```

In this example, the combination of the settings is (rw=randread, iodepth=1), (rw=randread, iodepth=2), ... The following
will be interpolated in the file `<%rw%>` and `<%iodepth%>` and replaced with their combination values.

### `environment`

Environment settings for manipulating the environment for workloads.

 * `enabled` (optional) - Whether to enable environment changes. Defaults to False.
   * **ex:** `enabled=1`
 * `nomerges` (optional) - Sets device merging to one of the following: 0 (enabled), 1 (some merging), 2 (disabled).
   * **ex:** `nomerges=0,1,2`
   * *NOTE: Setting is added as a workload setting combination.*

Environment settings can be added to the template `format` by either specifying them in a custom format, or by enabling
them with the `append_environment` setting under the `output` section.

### workloads

All other sections are considered a distinct workload that will be ran for each `device`, `scheduler`, and 
`template setting` combination for a given number of `repetitions`. The name of the section is considered the name of
the workload.

 * `file` (required) - The file to execute.
   * **ex:** `file=my-job.fio`
 * `enable_blktrace` (optional) - Executes blktrace alongside workload and gathers relevant metrics. Defaults to False.
   * **ex:** `enable_blktrace=1`

The file will be executed with the appropriate command given the `workload_type`. For example, `workload_type=fio` would
run `fio <file> --output-format=json`. While `workload_type=filebench` would run `filebench -f <file>`.

## Examples

Usage examples can be found under the [examples](https://github.com/UOFL-CSL/iobs/tree/master/examples) folder.

## Changelog

All changes and versioning information can be found in the [CHANGELOG](https://github.com/UOFL-CSL/iobs/tree/master/CHANGELOG.md).

## License

Copyright (c) 2018 UofL Computer Systems Lab. See [LICENSE](https://github.com/UOFL-CSL/iobs/tree/master/LICENSE) for details.
