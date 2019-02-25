#!/usr/bin/env python
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


import colorama
import signal
import sys

from iobs.cli import dispatch
from iobs.errors import IOBSBaseException
from iobs.output import printf, PrintType
from iobs.process import ProcessManager


def sig_handler(signal, frame):
    """Signal handler for termination signals sent to main process.

    Args:
        signal: The signal.
        frame: The frame.
    """
    printf('Program encountered termination signal, aborting...',
           print_type=PrintType.ERROR | PrintType.ERROR_LOG)

    ProcessManager.kill_processes()
    ProcessManager.clear_processes()

    colorama.deinit()

    sys.exit(1)


def main():
    try:
        return dispatch(sys.argv[1:])
    except IOBSBaseException as err:
        printf('Program encountered critical error\n{}'.format(err),
               print_type=PrintType.ERROR | PrintType.ERROR_LOG)
        return '{}: {}'.format(err.__class__.__name__, err.args[0])


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, sig_handler)  # Process termination
    signal.signal(signal.SIGINT, sig_handler)  # Keyboard interrupt

    colorama.init()

    sys.exit(main())
