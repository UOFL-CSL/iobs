#!/usr/bin/python3
# Utility for utilizing HOBOware files w/ iobs tool.
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

__author__ = 'Jared Gillespie'
__version__ = '0.1.0'

from time import mktime, strptime, struct_time

import os
import sys


class RowInfo:
    """A single row for a CSV."""

    def __init__(self, line, start_time, stop_time, io_kbytes):
        self.line = line
        self.joules = 0
        self.start_time = start_time
        self.stop_time = stop_time
        self.io_kbytes = io_kbytes

    def __str__(self):
        if self.io_kbytes == 0:
            return '%s,%0.2f' % (self.line, self.joules)
        else:
            return '%s,%0.2f,%0.2f' % (self.line, self.joules, self.io_kbytes / self.joules)


def search_single(hobo_file: str, start_time: struct_time, stop_time: struct_time):
    """Averages values in a HOBO file between a range.

    :param hobo_file: The HOBO file to search.
    :param start_time: The inclusive start of the average range.
    :param stop_time: The inclusive stop of the average range.
    """
    joules = 0
    lc = 0
    found = False
    with open(hobo_file, 'r') as file:
        for line in file:
            lc += 1

            # Skip first two lines (title + header)
            if lc < 3:
                continue

            _, date_time, _, _, w, _, _, _ = line.split(',')

            date_time = strptime(date_time, '%m/%d/%y %I:%M:%S %p')

            if start_time <= date_time <= stop_time:
                found = True
                joules += float(w)
            else:
                if start_time > date_time:
                    continue

                # Must have past stop time
                if not found:
                    print('Unable to find values within specified range!')
                    usage()
                    sys.exit(1)
                else:
                    break

        if not found:
            print('Unable to find values within specified range!')
            usage()
            sys.exit(1)

        print('Joules: %0.2f' % joules)
        print('Timespan: %ss' % int(mktime(stop_time) - mktime(start_time) + 1))


def search_csv(hobo_file: str, inp_file: str):
    """Calculates joules in a HOBO file for each row in the input file.

    :param hobo_file:
    :param inp_file:
    """
    row_infos = []
    header = ''
    start_index, stop_index, io_kbytes_index = -1, -1, -1
    lc = 0

    # Read in input file
    with open(inp_file, 'r') as file:
        for line in file:
            lc += 1

            # Find column indexes of start and stop
            if lc == 1:
                header = line.strip()
                start_index, stop_index, io_kbytes_index = search_header(line.strip())
                continue

            split = line.strip().split(',')
            start_time = strptime(split[start_index], '%m/%d/%y %I:%M:%S %p')
            stop_time = strptime(split[stop_index], '%m/%d/%y %I:%M:%S %p')
            if io_kbytes_index != -1:
                io_kbytes = float(split[io_kbytes_index])
            else:
                io_kbytes = 0
            row_infos.append(RowInfo(line.strip(), start_time, stop_time, io_kbytes))

    lc = 0

    # Read in HOBO file
    with open(hobo_file, 'r') as file:
        for line in file:
            lc += 1

            # Skip first two lines (title + header)
            if lc < 3:
                continue

            try:
                _, date_time, _, _, w, _, _, _ = line.strip().split(',')
            except:
                try:
                    _, date_time, _, _, w, _, _ = line.strip().split(',')
                except:
                    continue

            date_time = strptime(date_time, '%m/%d/%y %I:%M:%S %p')

            for row_info in row_infos:
                if row_info.start_time <= date_time <= row_info.stop_time:
                    row_info.joules += float(w)

    # Write output file
    with open(inp_file, 'w') as file:
        file.write(header + ',joules')
        if io_kbytes_index == -1:
            file.write('\n')
        else:
            file.write(',kbpj\n')

        for row_info in row_infos:
            file.write(str(row_info))
            file.write('\n')


def search_header(header: str):
    """Parses header for "start-time" and "stop-time" indexes.

    :param header: The header to parse.
    :return: A tuple containing the (start_index, stop_index, io_kbytes_index).
    """
    start_index, stop_index, io_kbytes_index = -1, -1, -1

    for index, column in enumerate(header.split(',')):
        if column == 'start-time':
            start_index = index
        elif column == 'stop-time':
            stop_index = index
        elif column == 'io-kbytes':
            io_kbytes_index = index

    if start_index == -1:
        raise Exception('Unable to parse header, expected "start-time"!')

    if stop_index == -1:
        raise Exception('Unable to parse header, expected "stop-time"!')

    return start_index, stop_index, io_kbytes_index


def usage():
    """Displays command-line information."""
    name = os.path.basename(__file__)
    print('%s %s' % (name, __version__))
    print('Usage: %s <hobo-file> <start-time> <stop-time>' % name)
    print('       %s <hobo-file> <inp-file>' % name)
    print('<hobo-file>       : Is expected to be a hobo output file with a title, a header, and rows with the following')
    print('                  : attributes: #, Date Time, V, A, W, Wh, Stopped, End Of File.')
    print('<start-time>      : Is the inclusive start of the range of values. Should be of the form MM/DD/YY HH:MM:SS PP.')
    print('<stop-time>       : Is the inclusive stop of the range of values. Should be of the form MM/DD/YY HH:MM:SS PP.')
    print('<inp-file>        : Is the input file to append columns to. Expects a header, command delimited, and at least')
    print('                  : two columns: start-time and stop-time.')
    print('Output: If <start-time> and <stop-time> are given, the average V, A, W, and Wh are given.')
    print('        Else, if <iobs-file> is given, the columns are added to each row.')


def main(argv: list):
    if '-h' in argv or '--help' in argv:
        usage()
        sys.exit(1)

    if len(argv) == 2:
        hobo_file, inp_file = argv

        if not os.path.isfile(hobo_file):
            print('HOBO file given does not exist: %s' % hobo_file)
            usage()
            sys.exit(1)

        if not os.path.isfile(inp_file):
            print('Input file given does not exist: %s' % inp_file)
            usage()
            sys.exit(1)

        search_csv(hobo_file, inp_file)
    elif len(argv) == 3:
        hobo_file, start_time, stop_time = argv

        if not os.path.isfile(hobo_file):
            print('HOBO file given does not exist: %s' % hobo_file)
            usage()
            sys.exit(1)

        try:
            start_time = strptime(start_time, '%m/%d/%y %I:%M:%S %p')
        except:
            print('Unable to parse given start time: %s' % start_time)
            usage()
            sys.exit(1)

        try:
            stop_time = strptime(stop_time, '%m/%d/%y %I:%M:%S %p')
        except:
            print('Unable to parse given stop time: %s' % stop_time)
            usage()
            sys.exit(1)

        search_single(hobo_file, start_time, stop_time)
    else:
        usage()
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
