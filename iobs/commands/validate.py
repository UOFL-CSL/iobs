# Copyright (c) 2018, UofL Computer Systems Lab.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without event the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


import argparse
import os
import platform

from iobs.errors import (
    InvalidOSError,
    InvalidPrivilegesError,
)
from iobs.input import parse_config_file
from iobs.output import printf, PrintType


def validate_os():
    """Checks whether the required operating system is in use.

    Raises:
        InvalidOSError: If OS is not Linux.
    """
    ps = platform.system()
    if ps != 'Linux':
        raise InvalidOSError('OS is {}, must be Linux.'.format(ps))


def validate_privileges():
    """Checks whether the script is ran with administrative privileges.

    Raises:
        InvalidPrivilegesError: If script isn't ran with sudo privileges.
    """
    if os.getuid() != 0:
        raise InvalidPrivilegesError(
            'Script must be run with administrative privileges.'
        )


def validate(args):
    """Validates workloads.

    Args:
        args: The parsed command-line arguments.

    Returns:
        0 if successful.

    Raises:
        IOBSBaseException: If error occurs and `continue_on_failure` not set.
    """
    validate_os()
    validate_privileges()

    printf('Beginning program validation...',
           print_type=PrintType.NORMAL | PrintType.INFO_LOG)

    for i, input_file in enumerate(args.inputs):
        printf('Validating input file {} ({} of {})'
               .format(input_file, i + 1, len(args.inputs)))

        configuration = parse_config_file(input_file)
        configuration.validate()

    printf('Finishing program validation...',
           print_type=PrintType.NORMAL | PrintType.INFO_LOG)

    return 0


def main(args):
    parser = argparse.ArgumentParser(prog='iobs validate')
    parser.add_argument(
        'inputs',
        nargs='+',
        metavar='input',
        help='The configuration files to validate.'
    )

    args = parser.parse_args(args)
    return validate(args)
