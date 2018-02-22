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
import pprint
import sys


class Mem:
    """A simple data-store for persisting and keeping track of global data."""

    # Settings
    devices = []
    schedulers = ['cfq', 'deadline', 'noop']
    log = False
    verbose = False


# region utils
def toggle_run(toggle: bool):
    """A decorator which determines whether to execute the function based on the
    supplied boolean parameter.

    :param toggle: Whether to execute the function.
    :return: The decorated function
    """
    def tags_decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if toggle:
                return func(*args, **kwargs)
            else:
                return None
        return func_wrapper
    return tags_decorator


@toggle_run(Mem.log)
def log(*args, **kwargs):
    """Prints a message if logging enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    logging.debug(*args, **kwargs)


@toggle_run(Mem.verbose)
def print_verbose(*args, **kwargs):
    """Prints a message if verbose enabled.

    :param args: The arguments.
    :param kwargs: The keyword arguments.
    """
    print(*args, **kwargs)

# endregion


# region command-line
def usage():
    """Displays command-line information."""
    name = os.path.basename(__file__)
    print('%s %s' % (name, __version__))
    print('Usage: %s -d <dev> [-s <sched>] [-l] [-v]' % name)
    print('Command Line Arguments:')
    print('-d <dev>          : The device to use (e.g. /dev/sda). Multiple devices can be given to run in sequence')
    print('                    (e.g. /dev/sda,/dev/sdb).')
    print('-s <sched>        : (OPTIONAL) The I/O scheduler to use (e.g. noop). Multiple schedulers can be given to')
    print('                    run in sequence (e.g. cfq,noop). Defaults to cfq, deadline, and noop.')
    print('-l                : (OPTIONAL) Logs debugging information to an iobs.log file.')
    print('-v                : (OPTIONAL) Prints verbose information to the STDOUT.')



def parse_args(argv: list) -> bool:
    """Parses the supplied arguments and persists in memory.

    :param argv: A list of arguments.
    :return: Returns a boolean as True if parsed correctly, otherwise False.
    """
    try:
        opts, args = getopt(argv, 'ld:s:v')

        for opt, arg in opts:
            if opt == '-d':
                Mem.devices.extend(try_split(arg, ','))
            elif opt == '-l':
                Mem.log = True
            elif opt == '-s':
                Mem.schedulers = try_split(arg, ',')
            elif opt == 'v':
                Mem.verbose = True
        return True
    except GetoptError as err:
        print(err)
        return False


def check_args() -> bool:
    """Validates that the minimum supplied arguments are met, and are valid.

    :return: Returns a boolean as True if requirements met, otherwise False.
    """
    if not Mem.devices:
        print('No devices given. Specify a device via -d <dev>')
        return False

    if not Mem.schedulers:  # Shouldn't occur due to defaults
        print('No schedulers given. Specify a scheduler via -s <sched>')
        return False

    # TODO: Validate devices given are valid block devices, and are mounted

    # TODO: Validate schedulers given are valid schedulers

    return True


def try_split(s: str, delimiter) -> list:
    if isinstance(delimiter, tuple):
        for d in delimiter:
            if d in s:
                return s.split(d)
    elif delimiter in s:
        return s.split(delimiter)

    return [s]
                         
# endregion


def main(argv):
    # Initialization
    logging.basicConfig(filename='iobs.txt', level=logging.DEBUG, format='%(asctime)s - %(message)s')


    # Validate arguments
    if not parse_args(argv):
        usage()
        return

    if not check_args():
        usage()
        return


if __name__ == '__main__':
    main(sys.argv[1:])
