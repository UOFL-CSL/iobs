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
import logging
import os
import platform

from iobs.errors import (
    InvalidOSError,
    InvalidPrivilegesError,
    IOBSBaseException
)
from iobs.input import parse_config_file
from iobs.output import printf, PrintType
from iobs.settings import SettingsManager


def check_args(args):
    """Checks the command-line arguments and sets settings.

    Args:
        args: The arguments to check.
    """
    SettingsManager.set('output_directory', args.output_directory)
    os.makedirs(args.output_directory, exist_ok=True)

    if args.log_file:
        SettingsManager.set('log_enabled', True)
        log_level = get_log_level(args.log_level)
        log_path = os.path.join(args.log_file)
        logging.basicConfig(filename=log_path, level=log_level,
                            format='%(asctime)s - %(message)s')
    else:
        SettingsManager.set('log_enabled', False)

    SettingsManager.set('continue_on_failure', args.continue_on_failure)
    SettingsManager.set('reset_device', args.reset_device)
    SettingsManager.set('retry_count', args.retry_count)
    SettingsManager.set('silent', args.silent)


def get_log_level(log_level):
    """Converts the `log_level` into logging level.

    Args:
        log_level: The level to convert.

    Returns:
        A logging level.
    """
    if log_level == 1:
        return logging.DEBUG
    if log_level == 2:
        return logging.INFO
    return logging.ERROR


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


def execute(args):
    """Executes workloads.

    Args:
        args: The parsed command-line arguments.

    Returns:
        0 if successful.

    Raises:
        IOBSBaseException: If error occurs and `continue_on_failure` not set.
    """
    check_args(args)
    validate_os()
    validate_privileges()

    printf('Beginning program execution...',
           print_type=PrintType.NORMAL | PrintType.INFO_LOG)

    for i, input_file in enumerate(args.inputs):
        try:
            printf('Processing input file {} ({} of {})'
                   .format(input_file, i + 1, len(args.inputs)))

            configuration = parse_config_file(input_file)
            configuration.validate()

            if args.reset_device:
                configuration.save_device_environments()

            configuration.process()
        except IOBSBaseException as err:
            if not SettingsManager.get('continue_on_failure'):
                raise err

            printf('input file {} failed all retries. Continuing execution '
                   'of remaining files...\n{}'.format(input_file, err),
                   print_type=PrintType.ERROR | PrintType.ERROR_LOG)
        finally:
            if args.reset_device:
                configuration.reset_device_environments()

    printf('Finishing program execution...',
           print_type=PrintType.NORMAL | PrintType.INFO_LOG)

    return 0


def main(args):
    parser = argparse.ArgumentParser(prog='iobs execute')
    parser.add_argument(
        'inputs',
        nargs='+',
        metavar='input',
        help='The configuration files to execute.'
    )
    parser.add_argument(
        '-o', '--output-directory',
        dest='output_directory',
        default=os.getcwd(),
        help='The output directory for output files. Defaults to the current '
             'working directory.'
    )
    parser.add_argument(
        '-l', '--log-file',
        dest='log_file',
        help='The file to log information to.'
    )
    parser.add_argument(
        '--log-level',
        dest='log_level',
        choices=[1, 2, 3],
        default=1,
        type=int,
        help='The level of information to which to log: 1 (Debug), '
             '2 (Info), 3 (Error). Defaults to 2.'
    )
    parser.add_argument(
        '-s', '--silent',
        default=False,
        action='store_true',
        help='Silences output to STDOUT.'
    )
    parser.add_argument(
        '-r', '--retry-count',
        dest='retry_count',
        default=1,
        type=int,
        help='Number of times to retry a failed workload. Defaults to 1.'
    )
    parser.add_argument(
        '-c', '--continue-on-failure',
        dest='continue_on_failure',
        default=False,
        action='store_true',
        help='If a input fails, continues executing other inputs; otherwise '
             'exits the program.'
    )
    parser.add_argument(
        '-d', '--reset-device',
        dest='reset_device',
        default=False,
        action='store_true',
        help='Resets the device to original settings after execution of each '
             'input.'
    )

    args = parser.parse_args(args)
    return execute(args)
