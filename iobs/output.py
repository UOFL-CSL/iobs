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


import enum
import logging

from colorama import Fore

from iobs.settings import SettingsManager


class PrintType(enum.IntEnum):
    # STDOUT
    NORMAL = 1 << 1
    WARNING = 1 << 2
    ERROR = 1 << 3

    # LOG File
    DEBUG_LOG = 1 << 4
    INFO_LOG = 1 << 5
    ERROR_LOG = 1 << 6


def printf(*args, print_type=PrintType.NORMAL, **kwargs):
    """Prints to STDOUT or log file depending on `print_type`.

    Args:
        args: Arguments to pass to the print and/or log function.
        print_type: Where and how to output the text.
        kwargs: Keyword arguments to pass to the print and/or log function.
    """
    args = [a.strip() if isinstance(a, str) else a for a in args]
    silent = SettingsManager.get('silent')
    log_enabled = SettingsManager.get('log_enabled')

    if not silent:
        if print_type & PrintType.NORMAL:
            print(*args, **kwargs)

        if print_type & PrintType.WARNING:
            print(*[Fore.YELLOW + str(a) + Fore.RESET for a in args], **kwargs)

        if print_type & PrintType.ERROR:
            print(*[Fore.RED + str(a) + Fore.RESET for a in args], **kwargs)

    if log_enabled:
        if print_type & PrintType.ERROR_LOG:
            logging.error(*args, **kwargs)

        if print_type & PrintType.INFO_LOG:
            logging.info(*args, **kwargs)

        if print_type & PrintType.DEBUG_LOG:
            logging.debug(*args, **kwargs)
