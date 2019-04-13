# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Job option "enable_blktrace". Runs blktrace and grabs relevant metrics.
- Output option "append_blktrace". Appends blktrace metrics to output.
- Output option "ignore_missing". If missing metric, "NONE" is used as value in output.

### Fixed
- Fixed output parsing exception being handled properly.

## [1.1.0] - 2019-03-18
### Added
- Environment section added. Allows adding settings which change the environment.
- Environment option "enabled". Enables environment configuration.
- Environment option "nomerges". Allows specifying "nomerges=0,1,2" for devices.
- Output option "append_environment". Appends environment settings to output.
- Filebench workload type added.
- Validation argument (i.e. `iobs validation`) to validate files without execution.
- `-d` `--reset-device` argument added. Resets device to prior execution state
after execution.

### Fixed
- Template settings should appear in output if specified in format.
- Fixed output file extension as ".csv".

## [1.0.0] - 2019-02-26
### Changed
- Rewrote as package
- Overhauled command-line arguments with "argparse".

### Added
- Templating section added for workload file configuration interpolation.
- Output section for output configuration.

### Removed
- Misc utility files.
- Obsolete example config files.

[Unreleased]: https://github.com/uofl-csl/iobs/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/uofl-csl/iobs/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/uofl-csl/iobs/compare/v1.0.0...v1.0.0
