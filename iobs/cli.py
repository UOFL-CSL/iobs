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
import pkg_resources
import setuptools

import colorama

import iobs


def _registered_commands(group='iobs.registered_commands'):
    """Retrieves registered commands for a entry point group.

    Args:
        group: The group.

    Returns:
        A dictionary mapping entry point name to entry point for each entry
        in the group.
    """
    return {c.name: c for c in pkg_resources.iter_entry_points(group=group)}


def list_dependencies_and_versions():
    """Retrieves a list of package dependencies and their current versions.

    Returns:
        List of tuples containing package dependency name and version.
    """
    return [
        ('colorama', colorama.__version__),
        ('setuptools', setuptools.__version__)
    ]


def dep_versions():
    """Retrieves string of package dependencies.

    Returns:
        String of package dependencies and their current versions.
    """
    return ', '.join(
        '{}: {}'.format(*dependency)
        for dependency in list_dependencies_and_versions()
    )


def dispatch(argv):
    """Dispatches execution to the appropriate command.

    Args:
        argv: The command-line arguments.

    Returns:
        Execution of the command.
    """
    registered_commands = _registered_commands()
    parser = argparse.ArgumentParser(prog='iobs')
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s versions {} ({})'.format(
            iobs.__version__,
            dep_versions()
        )
    )
    parser.add_argument(
        'command',
        choices=registered_commands.keys()
    )
    parser.add_argument(
        'args',
        help=argparse.SUPPRESS,
        nargs=argparse.REMAINDER
    )

    args = parser.parse_args(argv)

    main = registered_commands[args.command].load()
    return main(args.args)
