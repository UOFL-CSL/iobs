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
__version__ = '0.1.0'


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


class Mem:
    """A simple data-store for persisting and keeping track of global data."""

    # Settings
    devices: list = []
    log: bool = False
    runtime: int = 0
    schedulers: list = []
    verbose: bool = False
    workloads: list = []

    # Defaults
    def_schedulers: list = ['cfq', 'deadline', 'noop']
    def_workloads: list = ['rr', 'rw', 'sr', 'sw']

    # Regex
    re_device = re.compile(r'/dev/(.*)')


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
        logging.debug(*args, **kwargs)


def print_verbose(*args, **kwargs):
    """Prints a message if verbose is enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    if Mem.verbose:
        print(*args, **kwargs)


def print_detailed(*args, **kwargs):
    """Prints a message if verbose is enabled, and logs if logging is enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    log(*args, **kwargs)
    print_verbose(*args, **kwargs)


def run_parallel_commands(commands: list, max_concurrent: int=multiprocessing.cpu_count(), abort_on_failure: bool=True) -> [(str, int)]:
    """Runs multiple commands in parallel via subprocess communication.

    A single failed process results in the remaining being stopped.

    :param commands: The commands.
    :param max_concurrent: The maximum number of concurrent processes.
    :param abort_on_failure: Whether to abort if a single process failures, otherwise continues. Defaults to True.
    :return: A list of tuples containing (the output, the return code).
    """

    if max_concurrent < 1:
        print_detailed('Maximum concurrent processes must be > 0')
        return None

    processes = set()
    completed_processes = set()

    for command in sorted(commands):
        args = shlex.split(command)

        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                                 preexec_fn=os.setsid)
            processes.add(p)
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

        ret = []

        # Grab outputs from completed processes
        for process in completed_processes:
            out, err = process.communicate()

            rc = process.returncode

            if err:
                print_detailed(err.decode('utf-8'))

            ret.append((out.decode('utf-8'), rc))

        return ret

    # We got here because we aborted, continue the abortion...
    failed_processes = get_failed_processes(processes)
    print_processes(failed_processes)

    kill_processes(processes)

    return None


def run_command(command: str, inp: str='') -> (str, int):
    """Runs a command via subprocess communication.

    :param command: The command.
    :param inp: (OPTIONAL) Command input.
    :return: A tuple containing (the output, the return code).
    """
    args = shlex.split(command)

    try:
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


def try_split(s: str, delimiter) -> list:
    """Tries to split a string by the given delimiter(s).

    :param s: The string to split.
    :param delimiter: Either a single string, or a tuple of strings (i.e. (',', ';').
    :return: Returns the string split into a list.
    """
    if isinstance(delimiter, tuple):
        for d in delimiter:
            if d in s:
                return s.split(d)
    elif delimiter in s:
        return s.split(delimiter)

    return [s]


def get_failed_processes(processes: set) -> set:
    """Returns the processes which are failed.

    :param processes: The processes.
    :return: A set of failed processes.
    """
    failed_processes = set()

    for process in processes:
        rc = process.poll()

        if rc is not None:  # Done processing
            if rc != 0:  # Return code other than 0 indicates error
                failed_processes.add(process)

    return failed_processes


def get_finished_processes(processes: set) -> set:
    """Returns the processes which are finished.

    :param processes: The processes.
    :return: A set of finished processes.
    """
    finished_processes = set()

    for process in processes:
        rc = process.poll()

        if rc is not None:  # Done processing
            finished_processes.add(process)

    return finished_processes


def kill_processes(processes: set):
    """Kills the processes.

    :param processes: The processes.
    """
    for process in processes:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)


def print_processes(processes: set):
    """Prints the each processes's output.

    :param processes: The processes.
    """
    for process in processes:
        out, err = process.communicate()
        if err:
            print_detailed(err.decode('utf-8'))
        if out:
            print_detailed(out.decode('utf-8'))

# endregion


# region command-line
def usage():
    """Displays command-line information."""
    name = os.path.basename(__file__)
    print('%s %s' % (name, __version__))
    print('Usage: %s -d <dev> -r <runtime> [-s <sched>] [-w <workload>] [-l] [-v]' % name)
    print('Command Line Arguments:')
    print('-d <dev>          : The device to use (e.g. /dev/sda). Multiple devices can be given to run in sequence')
    print('                    (e.g. /dev/sda,/dev/sdb).')
    print('-r <runtime>      : The runtime (seconds) for running the traces.')
    print('-s <sched>        : (OPTIONAL) The I/O scheduler to use (e.g. noop). Multiple schedulers can be given to')
    print('                    run in sequence (e.g. cfq,noop). Defaults to cfq, deadline, and noop for HDDs and SSDs.')
    print('                    NVMe drives don\'t use a scheduler, but use blkmq instead.')
    print('-w <workload>     : (OPTIONAL) The workload to use (e.g rw). Multiple workloads can be given to run in')
    print('                    sequence (e.g. rw,sw). Four workloads are available: rr (random read),')
    print('                    rw (random write), sr (sequential read), sw (sequential write). Defaults to all four.')
    print('-l                : (OPTIONAL) Logs debugging information to an iobs.log file.')
    print('-v                : (OPTIONAL) Prints verbose information to the STDOUT.')


def parse_args(argv: list) -> bool:
    """Parses the supplied arguments and persists in memory.

    :param argv: A list of arguments.
    :return: Returns a boolean as True if parsed correctly, otherwise False.
    """
    try:
        opts, args = getopt(argv, 'ld:r:s:v')

        for opt, arg in opts:
            if opt == '-d':
                Mem.devices.extend(try_split(arg, ','))
            elif opt == '-l':
                Mem.log = True
            elif opt == '-r':
                Mem.runtime = ignore_exception(ValueError, 0)(int)(arg)
            elif opt == '-s':
                Mem.schedulers.extend(try_split(arg, ','))
            elif opt == '-v':
                Mem.verbose = True
            elif opt == '-w':
                Mem.workloads.extend(try_split(arg, ','))
        return True
    except GetoptError as err:
        print_verbose(err)
        return False


def check_args() -> bool:
    """Validates that the minimum supplied arguments are met, and are valid.

    :return: Returns a boolean as True if requirements met, otherwise False.
    """
    # Check devices
    if not Mem.devices:
        print_detailed('No devices given. Specify a device via -d <dev>.')
        return False

    for device in Mem.devices:
        if not is_block_device(device):
            print_detailed('The device %s is not a valid block device.' % device)
            return False

    # Check runtime
    if not Mem.runtime:
        print_detailed('A runtime (seconds) must be given. Specify a runtime via -r <runtime>.')
        return False

    # Check schedulers
    if not Mem.schedulers:  # Use defaults if none specified
        Mem.schedulers = Mem.def_schedulers

    # Validates schedulers against all devices, HDDs and SSDs should allow all provided schedulers.
    # TODO: Validate whether it is appropriate to force this restriction
    for device in Mem.devices:
        if is_nvme(device):
            continue

        schedulers = get_schedulers(device)

        for scheduler in Mem.schedulers:
            if scheduler not in schedulers:
                print_detailed('Invalid scheduler %s specified for device %s' % (scheduler, device))
                return False

    # Check workloads
    if not Mem.workloads:  # Use defaults if none specified
        Mem.workloads = Mem.def_workloads

    for workload in Mem.workloads:
        if workload not in Mem.def_workloads:
            print_detailed('Invalid workload %s specified.' % workload)
            return False

    return True


def check_commands() -> bool:
    """Validates whether the required commands exists on the system.

    :return: Returns True if commmands exists, else False.
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

    if not command_exists('fio'):
        print_detailed('fio is not installed. Please install via \'sudo apt install fio\'')
        return False

    return True
# endregion


# region commands
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


@ignore_exception(FileNotFoundError, False)
@ignore_exception(TypeError, False)
def is_block_device(device: str) -> bool:
    """Returns whether the given device is a valid block device.

    :param device: The device.
    :return: Returns True if is a valid block device, else False.
    """
    info = os.stat(device)
    return stat.S_ISBLK(info.st_mode)


def is_nvme(device: str) -> bool:
    """Returns whether the given device is an NVMe device.

    :param device: The device.
    :return: Returns True if is an NVMe device, else False.
    """
    matches = Mem.re_device.findall(device)

    if not matches:
        return False

    out, rc = run_command('cat /sys/block/%s/queue/scheduler' % matches[0])

    if rc != 0:
        return False

    return out == 'none'


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
# endregion


def main(argv):
    # Validate os
    ps = platform.system()
    if ps != 'Linux':
        sys.exit('OS is %s, must be Linux.' % ps)

    # Validate arguments
    if not parse_args(argv):
        usage()
        sys.exit(1)

    if Mem.log:
        logging.basicConfig(filename='iobs.txt', level=logging.DEBUG, format='%(asctime)s - %(message)s')

    if not check_args():
        usage()
        sys.exit(1)

    if not check_commands():
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
