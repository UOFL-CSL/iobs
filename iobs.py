#!/usr/bin/python3
# Linux I/O Benchmark for Schedulers
# Copyright (c) 2018, UofL Computer Systems Lab.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without event the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Jared Gillespie'
__version__ = '0.2.0'


from functools import wraps
from getopt import getopt, GetoptError

import logging
import os
import platform
import multiprocessing
import re
import stat
import shlex
import signal
import subprocess
import sys
import time


# TODO: Code needs refactoring for a more OO approach.
# TODO: Add more logging around functions (specifically in the iobs.log file.
# TODO: Reorganize methods in a more structured manner.


class Mem:
    """A simple data-store for persisting and keeping track of global data."""

    def __init__(self):
        # Constants
        self.GLOBAL_HEADER = 'globals'

        # Settings
        self.cleanup: bool = False
        self.config_file: str = None
        self.continue_on_failure: bool = False
        self.jobs: list = []
        self.log: bool = False
        self.verbose: bool = False

        # Global Job Settings
        self._command: str = None
        self._delay: int = 0
        self._device: str = None
        self._repetition: int = 1
        self._runtime: int = None
        self._schedulers: set = None
        self._workload: str = None

        # Formatters
        self.format_blktrace = 'blktrace -d %s -o %s -w %s'  # device, file prefix, runtime
        self.format_blkparse = 'blkparse -i %s.blktrace.* -d %s.blkparse.bin'  # file prefix, file prefix
        self.format_btt = 'btt -i %s.blkparse.bin'  # file prefix

        # Regex
        self.re_btt_d2c = re.compile(r'D2C\s*(?:\d+.\d+)\s*(\d+.\d+)\s*(?:\d+.\d+)\s*(?:\d+)')
        self.re_btt_q2c = re.compile(r'Q2C\s*(?:\d+.\d+)\s*(\d+.\d+)\s*(?:\d+.\d+)\s*(?:\d+)')
        self.re_device = re.compile(r'/dev/(.*)')
        self.re_fio_clat_msec = re.compile(
            r'clat \(msec\): min=(?:\d+.?\d*), max=(?:\d+.?\d*), avg=(\d+.?\d*), stdev=(?:\d+.?\d*)')
        self.re_fio_clat_usec = re.compile(
            r'clat \(usec\): min=(?:\d+.?\d*), max=(?:\d+.?\d*), avg=(\d+.?\d*), stdev=(?:\d+.?\d*)')
        self.re_fio_slat_msec = re.compile(
            r'slat \(msec\): min=(?:\d+.?\d*), max=(?:\d+.?\d*), avg=(\d+.?\d*), stdev=(?:\d+.?\d*)')
        self.re_fio_slat_usec = re.compile(
            r'slat \(usec\): min=(?:\d+.?\d*), max=(?:\d+.?\d*), avg=(\d+.?\d*), stdev=(?:\d+.?\d*)')

        # Validity
        self.valid_global_settings = {'command', 'delay', 'device', 'schedulers', 'repetition', 'runtime', 'workload'}
        self.valid_job_settings = {'command', 'delay', 'device', 'schedulers', 'repetition', 'runtime', 'workload'}
        self.valid_workloads = {'fio'}

    @property
    def command(self) -> str:
        return self._command

    @command.setter
    def command(self, value: str):
        self._command = value

    @property
    def delay(self) -> int:
        return self._delay

    @delay.setter
    def delay(self, value: int):
        conv_value = ignore_exception(ValueError, -1)(int)(value)

        if conv_value < 1:
            raise ValueError('Delay given is < 0: %s' % value)

        self._delay = value

    @property
    def device(self) -> str:
        return self._device

    @device.setter
    def device(self, value: str):
        self._device = value

    @property
    def repetition(self) -> int:
        return self._repetition

    @repetition.setter
    def repetition(self, value: int):
        conv_value = ignore_exception(ValueError, 0)(int)(value)

        if conv_value < 1:
            raise ValueError('Repetition given is < 1: %s' % value)

        self._repetition = conv_value

    @property
    def runtime(self):
        return self._runtime

    @runtime.setter
    def runtime(self, value: int):
        conv_value = ignore_exception(ValueError, 0)(int)(value)

        if conv_value < 1:
            raise ValueError('Runtime given is < 1: %s' % value)

        self._runtime = conv_value

    @property
    def schedulers(self) -> set:
        return self._schedulers

    @schedulers.setter
    def schedulers(self, value):
        self._schedulers = set(try_split(value, ','))

    @property
    def workload(self) -> str:
        return self._workload

    @workload.setter
    def workload(self, value: str):
        self._workload = value


# Turns the class into a singleton (this is some sneaky stuff)
Mem = Mem()


class Job:
    """A single job, which is representative of a single workload to be run."""

    def __init__(self, name: str):
        self._name: str = name
        self._command: str = None
        self._delay: int = None
        self._device: str = None
        self._repetition: int = None
        self._runtime: int = None
        self._schedulers: set = None
        self._workload: str = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def command(self) -> str:
        return self._command

    @command.setter
    def command(self, value: str):
        self._command = value

    @property
    def delay(self) -> int:
        return self._delay

    @delay.setter
    def delay(self, value: int):
        conv_value = ignore_exception(ValueError, -1)(int)(value)

        if conv_value < 1:
            raise ValueError('Delay given is < 0: %s' % value)

        self._delay = conv_value

    @property
    def device(self) -> str:
        return self._device

    @device.setter
    def device(self, value):
        self._device = value

    @property
    def repetition(self) -> int:
        return self._repetition

    @repetition.setter
    def repetition(self, value: int):
        conv_value = ignore_exception(ValueError, 0)(int)(value)

        if conv_value < 1:
            raise ValueError('Repetition given is < 1: %s' % value)

        self._repetition = conv_value

    @property
    def runtime(self) -> int:
        return self._runtime

    @runtime.setter
    def runtime(self, value: int):
        conv_value = ignore_exception(ValueError, 0)(int)(value)

        if conv_value < 1:
            raise ValueError('Runtime given is < 1: %s' % value)

        self._runtime = conv_value

    @property
    def schedulers(self) -> set:
        return self._schedulers

    @schedulers.setter
    def schedulers(self, value):
        self._schedulers = set(try_split(value, ','))

    @property
    def workload(self) -> str:
        return self._workload

    @workload.setter
    def workload(self, value):
        self._workload = value

    def fill_missing(self, o):
        """Fills in missing values from object.

        :param o: The object.
        """
        if self._delay is None:
            self._delay = ignore_exception(AttributeError)(getattr)(o, 'delay')

        if self._device is None:
            self._device = ignore_exception(AttributeError)(getattr)(o, 'device')

        if self._repetition is None:
            self._repetition = ignore_exception(AttributeError)(getattr)(o, 'repetition')

        if self._runtime is None:
            self._runtime = ignore_exception(AttributeError)(getattr)(o, 'runtime')

        if self._schedulers is None:
            self._schedulers = ignore_exception(AttributeError)(getattr)(o, 'schedulers')

        if self._workload is None:
            self._workload = ignore_exception(AttributeError)(getattr)(o, 'workload')

    def is_valid(self) -> bool:
        """Returns whether the job is valid.

        :return: Returns True if valid, else False.
        """
        return self._delay is not None and \
            self._device is not None and \
            self._repetition is not None and \
            self._runtime is not None and \
            self._schedulers is not None and \
            self._workload is not None

    def get_invalid_props(self) -> list:
        """Returns the properties that are invalid.

        :return: A list of properties.
        """
        invalid_props = []

        if self._delay is None:
            invalid_props.append('delay')

        if self._device is None:
            invalid_props.append('device')

        if self._repetition is None:
            invalid_props.append('repetition')

        if self._runtime is None:
            invalid_props.append('runtime')

        if self._schedulers is None:
            invalid_props.append('schedulers')

        if self._workload is None:
            invalid_props.append('workload')

        return invalid_props


class Metrics:
    """A group of metrics for a particular workload."""

    def __init__(self, workload: str):
        self.workload: str = workload
        self._metrics: list = []

    def add_metrics(self, metrics: dict):
        """Adds new metrics.

        :param metrics: The metrics. Expects mapping of metric name to metric value (int or float)."""
        self._metrics.append(metrics)

    def average_metrics(self) -> dict:
        """Averages the metrics into a new dictionary.

        :return: The averaged metrics.
        """
        averaged_metrics = dict()  # The averaged metrics
        metric_frequency = dict()  # The frequency of each metric

        # Sums the metrics then divides each by their frequency
        for metric in self._metrics:
            for key, value in metric.items():
                metric_frequency.setdefault(key, 0)
                metric_frequency[key] += 1

                averaged_metrics.setdefault(key, 0)
                averaged_metrics[key] += value

        for key in averaged_metrics:
            averaged_metrics[key] = averaged_metrics[key] / metric_frequency[key]

        return averaged_metrics

    @staticmethod
    def gather_workload_metrics(workload_out: str, workload: str) -> dict:
        """Parses workload outputs and returns relevant metrics.

        :param workload_out: The workload output.
        :param workload: The workload.
        :return: A dictionary of metrics and their values.
        """
        ret = dict()
        if workload == 'fio':
            # fio can give msec or usec output, check for whichever is found
            clat = Mem.re_fio_clat_msec.findall(workload_out)

            if clat:
                ret['clat'] = float(clat[0]) * 1e-3
            else:
                clat = Mem.re_fio_clat_usec.findall(workload_out)
                ret['clat'] = float(clat[0]) * 1e-6

            slat = Mem.re_fio_slat_msec.findall(workload_out)

            if slat:
                ret['slat'] = float(slat[0]) * 1e-3
            else:
                slat = Mem.re_fio_slat_usec.findall(workload_out)
                ret['slat'] = float(slat[0]) * 1e-6
        else:
            print_detailed('Unable to interpret workload %s' % workload)

        return ret

    @staticmethod
    def gather_metrics(blktrace_out: str, blkparse_out: str, btt_out: str, workload_out: str, workload: str) -> dict:
        """Parses command outputs and returns relevant metrics.

        :param blktrace_out: The blktrace command output.
        :param blkparse_out: The blkparse command output.
        :param btt_out: The btt command output.
        :param workload_out: The workload output.
        :param workload: The workload.
        :return: A dictionary of metrics and their values.
        """
        metrics = dict()

        d2c = Mem.re_btt_d2c.findall(btt_out)

        if d2c:
            metrics['d2c'] = float(d2c[0])

        q2c = Mem.re_btt_q2c.findall(btt_out)

        if q2c:
            metrics['q2c'] = float(q2c[0])

        workload_metrics = Metrics.gather_workload_metrics(workload_out, workload)

        metrics = {**metrics, **workload_metrics}

        return metrics


# region utils
def ignore_exception(exception=Exception, default_val=None):
    """A decorator function that ignores the exception raised, and instead returns a default value.

    :param exception: The exception to catch.
    :param default_val: The default value.
    :return: The decorated function.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception:
                return default_val
        return wrapper
    return decorator


def log(*args, **kwargs):
    """Logs a message if logging is enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    if Mem.log:
        args = [a.strip() if isinstance(a, str) else a for a in args]
        logging.debug(*args, **kwargs)


def print_verbose(*args, **kwargs):
    """Prints a message if verbose is enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    if Mem.verbose:
        args = [a.strip() if isinstance(a, str) else a for a in args]
        print(*args, **kwargs)


def print_detailed(*args, **kwargs):
    """Prints a message if verbose is enabled, and logs if logging is enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    log(*args, **kwargs)
    print_verbose(*args, **kwargs)


def try_split(s: str, delimiter) -> list:
    """Tries to split a string by the given delimiter(s).

    :param s: The string to split.
    :param delimiter: Either a single string, or a tuple of strings (i.e. (',', ';').
    :return: Returns the string split into a list.
    """
    if isinstance(delimiter, tuple):
        for d in delimiter:
            if d in s:
                return [i.strip() for i in s.split(d)]
    elif delimiter in s:
        return s.split(delimiter)

    return [s]


def get_failed_processes(processes: set) -> set:
    """Returns the processes which are failed.

    :param processes: A set of tuples of command names and processes.
    :return: A set of failed processes.
    """
    failed_processes = set()

    for command_name, process in processes:
        rc = process.poll()

        if rc is not None:  # Done processing
            if rc != 0:  # Return code other than 0 indicates error
                failed_processes.add((command_name, process))

    return failed_processes


def get_finished_processes(processes: set) -> set:
    """Returns the processes which are finished.

    :param processes: A set of tuples of command names and processes.
    :return: A set of finished processes.
    """
    finished_processes = set()

    for command_name, process in processes:
        rc = process.poll()

        if rc is not None:  # Done processing
            finished_processes.add((command_name, process))

    return finished_processes


def is_valid_setting(setting: str, header: str) -> bool:
    """Returns whether the config setting is valid.

    :return: Returns True if setting is valid, else False.
    """
    if not header:
        return False

    if not setting:
        return False

    if header == Mem.GLOBAL_HEADER:
        return setting in Mem.valid_global_settings
    else:
        return setting in Mem.valid_job_settings


def is_valid_workload(workload: str) -> bool:
    """Returns whether the given workload is valid.

    :param workload: The workload.
    :return: Returns True if valid, else False.
    """
    if workload not in Mem.valid_workloads:
        return False

    if not command_exists(workload):
        return False

    return True


def kill_processes(processes: set):
    """Kills the processes.

    :param processes: A set of tuples of command names and processes.
    """
    for command_name, process in processes:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)


def print_processes(processes: set):
    """Prints the each processes's output.

    :param processes: A set of tuples of command names and processes.
    """
    for command_name, process in processes:
        out, err = process.communicate()
        if out:
            print_detailed(out.decode('utf-8'))
        if err:
            print_detailed(err.decode('utf-8'))


def validate_jobs() -> bool:
    """Returns whether each job is valid.

    :return: Returns True if all are valid, else False.
    """
    job_index = 0
    while job_index < len(Mem.jobs):
        job = Mem.jobs[job_index]

        # Fill in missing settings from globals
        job.fill_missing(Mem)

        # Ensure job has required properties
        if not job.is_valid():
            ip = ', '.join(job.get_invalid_props())
            print_detailed('Job %s is missing the required settings: %s' % (job.name, ip))
            if Mem.continue_on_failure:
                Mem.jobs.pop(job_index)
                continue
            else:
                return False

        if not is_valid_workload(job.workload):
            print_detailed('%s is not installed. Please install the tool before use.' % job.workload)
            if Mem.continue_on_failure:
                Mem.jobs.pop(job_index)
                continue
            else:
                return False

        if not is_block_device(job.device):
            print_detailed('The device %s is not a valid block device.' % job.device)
            if Mem.continue_on_failure:
                Mem.jobs.pop(job_index)
                continue
            else:
                return False

        # We'll allow schedulers to be defined that don't exist for every device
        # So no checks here...

        job_index += 1

    return len(Mem.jobs) > 0  # At least 1 job required
# endregion


# region command-line
def usage():
    """Displays command-line information."""
    name = os.path.basename(__file__)
    print('%s %s' % (name, __version__))
    print('Usage: %s <file> [-l] [-v]' % name)
    print('Command Line Arguments:')
    print('<file>            : The configuration file to use.')
    print('-c                : (OPTIONAL) The application will continue in the case of a job failure.')
    print('-l                : (OPTIONAL) Logs debugging information to an iobs.log file.')
    print('-v                : (OPTIONAL) Prints verbose information to the STDOUT.')
    print('-x                : (OPTIONAL) Attempts to clean up intermediate files.')


def parse_args(argv: list) -> bool:
    """Parses the supplied arguments and persists in memory.

    :param argv: A list of arguments.
    :return: Returns a boolean as True if parsed correctly, otherwise False.
    """
    try:
        opts, args = getopt(argv, 'hlvx')

        for opt, arg in opts:
            if opt == '-c':
                Mem.continue_on_failure = True
            elif opt == '-h':
                return False
            elif opt == '-l':
                Mem.log = True
            elif opt == '-v':
                Mem.verbose = True
            elif opt == '-x':
                Mem.cleanup = True
        return True
    except GetoptError as err:
        print_detailed(err)
        return False


def parse_config_file(file_path: str) -> bool:
    """Parses the supplied file and persists data into memory.

    :param file_path: The file.
    :return: Returns True if settings are valid, else False.
    """
    Mem.config_file = file_path

    if not os.path.isfile(Mem.config_file):
        sys.exit('File not found: %s' % Mem.config_file)

    re_header = re.compile(r'\s*\[(.*)\]\s*(?:#.*)*')

    header = None

    with open(Mem.config_file, 'r') as file:
        for line in file:
            # Header
            header_match = re_header.fullmatch(line)

            if header_match:
                header = line[header_match.regs[1][0]:header_match.regs[1][1]]

                if header != Mem.GLOBAL_HEADER:
                    Mem.jobs.append(Job(header))
                continue

            # Comment
            comment_index = line.find('#')

            if comment_index != -1:
                line = line[0:comment_index].strip()

            if not line.strip():
                continue

            # Setting
            line_split = line.split('=')

            if len(line_split) != 2:  # Invalid syntax
                print_detailed('Invalid syntax in config file found: %s' % line)
                return False

            name = line_split[0].strip()
            value = line_split[1].strip()

            if not name or not value:
                print_detailed('Invalid syntax in config file found: %s' % line)
                return False

            if not is_valid_setting(name, header):
                print_detailed('Invalid syntax in config file found: %s' % line)
                return False

            if not header:
                print_detailed('Invalid syntax in config file found: %s' % line)
                return False

            if header == Mem.GLOBAL_HEADER:
                try:
                    setattr(Mem, name, value)
                except ValueError:
                    print_detailed('Invalid syntax in config file found: %s' % line)
                    return False
            else:
                try:
                    setattr(Mem.jobs[-1], name, value)
                except ValueError:
                    print_detailed('Invalid syntax in config file found: %s' % line)
                    return False
    return True
# endregion


# region commands
def cleanup_files(*files):
    """Removes the specified file, or files if multiple are given.

    :param files: Files to remove..
    """
    if not Mem.cleanup:  # Only cleanup if specified
        return

    for file in files:
        run_system_command('rm -f %s' % file)


def get_device_major_minor(device: str) -> str:
    """Returns a string of the major, minor of a given device.

    :param device: The device.
    :return: A string of major,minor.
    """
    out, _ = run_command('stat -c \'%%t,%%T\' %s' % device)

    return out if not out else out.strip()


def check_trace_commands() -> bool:
    """Validates whether the required tracing commands exists on the system.

    :return: Returns True if commands exists, else False.
    """
    if not command_exists('blktrace'):
        print_detailed('blktrace is not installed. Please install via \'sudo apt install blktrace\'')
        return False

    if not command_exists('blkparse'):  # Included with blktrace
        print_detailed('blkparse is not installed. Please install via \'sudo apt install blktrace\'')
        return False

    if not command_exists('btt'):  # Included with blktrace
        print_detailed('btt is not installed. Please install via \'sudo apt install blktrace\'')
        return False

    return True


def command_exists(command: str) -> bool:
    """Returns whether the given command exists on the system.

    :param command: The command.
    :return: Returns True if exists, else False.
    """
    rc = run_system_command('command -v %s' % command)

    return rc == 0


def get_schedulers(device: str) -> list:
    """Returns a list of available schedulers for a given device.

    :param device: The device.
    :return: Returns a list of schedulers.
    """
    matches = Mem.re_device.findall(device)

    if not matches:
        return []

    out, rc = run_command('cat /sys/block/%s/queue/scheduler' % matches[0])

    if rc != 0:
        return []

    return out.replace('[', '').replace(']', '').split()


def get_valid_schedulers(device: str, proposed_schedulers: list) -> list:
    """Returns a list of schedulers that are valid for a given device and set of proposed schedulers.

    :param device: The device.
    :param proposed_schedulers: The proposed schedulers.
    :return: Returns a list of schedulers.
    """
    valid_schedulers = []

    available_schedulers = set(get_schedulers(device))

    for scheduler in proposed_schedulers:
        if scheduler in available_schedulers:
            valid_schedulers.append(scheduler)

    return valid_schedulers


@ignore_exception(FileNotFoundError, False)
@ignore_exception(TypeError, False)
def is_block_device(device: str) -> bool:
    """Returns whether the given device is a valid block device.

    :param device: The device.
    :return: Returns True if is a valid block device, else False.
    """
    info = os.stat(device)
    return stat.S_ISBLK(info.st_mode)


def is_rotational_device(device: str) -> bool:
    """Returns whether the given device is a rotational device.

    :param device: The device.
    :return: Returns True if is a rotational device, else False.
    """
    matches = Mem.re_device.findall(device)

    if not matches:
        return False

    out, rc = run_command('cat /sys/block/%s/queue/rotational' % matches[0])

    if rc != 0:
        return False

    return int(out) == 1


def run_command(command: str, inp: str='') -> (str, int):
    """Runs a command via subprocess communication.

    :param command: The command.
    :param inp: (OPTIONAL) Command input.
    :return: A tuple containing (the output, the return code).
    """
    try:
        args = shlex.split(command)

        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                             preexec_fn=os.setsid)

        out, err = p.communicate(inp)

        rc = p.returncode

        if err:
            print_detailed(err.decode('utf-8'))

        return out.decode('utf-8'), rc
    except (ValueError, subprocess.CalledProcessError, FileNotFoundError) as err:
        print_detailed(err)
        return None, None


def run_parallel_commands(command_map: list, max_concurrent: int=multiprocessing.cpu_count(),
                          abort_on_failure: bool=True):
    """Runs multiple commands in parallel via subprocess communication. Commands are run in order of delay, with their
    respective delays considered (useful when commands like fio take time to generate a file before running).

    A single failed process results in the remaining being stopped.

    :param command_map: A command mapping which contains a list of tuples containing (command name, command delay,
        the command itself).
    :param max_concurrent: The maximum number of concurrent processes.
    :param abort_on_failure: Whether to abort if a single process failures, otherwise continues. Defaults to True.
    :return: A dictionary where key = command name and value = tuple of (the output, the return code).
    """

    if max_concurrent < 1:
        print_detailed('Maximum concurrent processes must be > 0')
        return None

    processes = set()
    completed_processes = set()

    last_delay = 0

    for command_name, delay, command in sorted(command_map, key=lambda x: x[1]):

        try:
            # Delay command execution based on specified delay
            # Note: This isn't quite exact, due to timing issues and the concurrency limit
            if delay > last_delay:
                time.sleep(delay - last_delay)
                last_delay = delay

            args = shlex.split(command)

            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                                 preexec_fn=os.setsid)

            processes.add((command_name, p))
        except (ValueError, subprocess.CalledProcessError, FileNotFoundError) as err:
            print_detailed(err)
            if abort_on_failure:
                break

        # Limit the number of threads
        while len(processes) >= max_concurrent:
            time.sleep(0.5)

            finished_processes = get_finished_processes(processes)

            processes.difference_update(finished_processes)
            completed_processes.update(finished_processes)

            if abort_on_failure:
                failed_processes = get_failed_processes(finished_processes)

                if failed_processes:  # Something failed, abort!
                    print_processes(failed_processes)
                    kill_processes(processes)
                    break
    else:
        # Wait for processes to finish
        while len(processes) > 0:
            time.sleep(0.5)

            finished_processes = get_finished_processes(processes)

            processes.difference_update(finished_processes)
            completed_processes.update(finished_processes)

            if abort_on_failure:
                failed_processes = get_failed_processes(finished_processes)

                if failed_processes:  # Something failed, abort!
                    print_processes(failed_processes)
                    kill_processes(processes)
                    return None

        ret = dict()

        # Grab outputs from completed processes
        for command_name, process in completed_processes:
            out, err = process.communicate()

            rc = process.returncode

            if err:
                print_detailed(err.decode('utf-8'))

            ret[command_name] = (out.decode('utf-8'), rc)

        return ret

    # We got here because we aborted, continue the abortion...
    failed_processes = get_failed_processes(processes)
    print_processes(failed_processes)

    kill_processes(processes)

    return None


def run_system_command(command: str, silence: bool=True) -> int:
    """Runs a system command.

    :param command: The command.
    :param silence: (OPTIONAL) Whether to silence the console output. Defaults to True.
    :return: The return code.
    """

    if silence:
        command = '%s >/dev/null 2>&1' % command
    rc = os.system(command)
    return rc
# endregion


def process_jobs():
    """Executes each job."""
    for job in Mem.jobs:
        if not execute_job(job):
            if not Mem.continue_on_failure:
                return


def execute_job(job: Job) -> bool:
    """Executes a single job.

    :param job: The job.
    :return: Returns True if successful, else False.
    """
    for scheduler in job.schedulers:

        if not change_scheduler(scheduler, job.device):
            print_detailed('Unable to change scheduler %s for device %s' % (scheduler, job.device))
            return False

        metrics = execute_workload(job.repetition, job.workload, job.delay, job.device, scheduler, job.runtime, job.command)

        # TODO: Print metrics or something

    return True


def execute_workload(repetition: int, workload: str, delay: int, device: str, scheduler: str, runtime: int, command: str):
    """Executes a workload.

    :param repetition: The number of times to repeat the workload.
    :param workload: The workload.
    :param delay: The delay.
    :param device: The device.
    :param scheduler: The schedulers.
    :param runtime: The runtime.
    :param command: The command.
    :return: Returns a dictionary of metrics if successful, else None.
    """
    metrics = Metrics(workload)

    # Repeat job multiple times
    for i in range(repetition):
        device_short = Mem.re_device.findall(device)[0]

        # Run workload along with blktrace
        blktrace = Mem.format_blktrace % (device, device_short, runtime)

        out = run_parallel_commands([('blktrace', 0, blktrace), (workload, delay, command)])

        # Error running commands
        if out is None:
            print_detailed('Error running commands')
            return None

        blktrace_out, _ = out['blktrace']
        workload_out, _ = out[workload]

        # Run blkparse
        blkparse = Mem.format_blkparse % (device_short, device_short)

        blkparse_out, _ = run_command(blkparse)

        # Run btt
        btt = Mem.format_btt % device_short

        btt_out, _ = run_command(btt)

        # Cleanup intermediate files
        cleanup_files('sda.blktrace.*', 'sda.blkparse.*', 'sys_iops_fp.dat', 'sys_mbps_fp.dat')

        dmm = get_device_major_minor(device)
        cleanup_files('%s_iops_fp.dat' % dmm, '%s_mbps_fp.dat' % dmm)

        m = Metrics.gather_metrics(blktrace_out, blkparse_out, btt_out, workload_out, workload)
        metrics.add_metrics(m)

    return metrics.average_metrics()


def change_scheduler(scheduler: str, device: str):
    """Changes the I/O scheduler for the given device.

    :param scheduler: The I/O scheduler.
    :param device: The device.
    :return: Returns True if successful, else False.
    """
    command = 'bash -c "echo %s > /sys/block/%s/queue/scheduler"' % (scheduler, Mem.re_device.findall(device)[0])

    out, rc = run_command(command)

    return rc == 0


def main(argv: list):
    # Set logging as early as possible
    if '-l' in argv:
        logging.basicConfig(filename='iobs.log', level=logging.DEBUG, format='%(asctime)s - %(message)s')
        Mem.log = True

    if '-v' in argv:
        Mem.verbose = True

    # Validate privileges
    if os.getuid() != 0:
        print_detailed('Script must be run with administrative privileges. Try sudo %s' % __file__)
        sys.exit(1)

    # Validate os
    ps = platform.system()
    if ps != 'Linux':
        print_detailed('OS is %s, must be Linux' % ps)
        sys.exit(1)

    if len(argv) == 0:
        usage()
        sys.exit(1)

    # Validate settings
    if not parse_config_file(argv[0]):
        sys.exit(1)

    if not validate_jobs():
        sys.exit(1)

    # Validate arguments
    if not parse_args(argv[1:]):
        usage()
        sys.exit(1)

    if not check_trace_commands():
        sys.exit(1)

    # Beginning running jobs
    process_jobs()


if __name__ == '__main__':
    main(sys.argv[1:])
