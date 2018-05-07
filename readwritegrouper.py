#!/usr/bin/python3
# Utility for utilizing grouping iobs output workloads of read / write only into single columns.
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

import os
import sys


def aggregate_csv(inp_file: str):
    """Calculates joules in a HOBO file for each row in the input file.

    :param inp_file:
    """
    new_header = ''
    new_lines = []
    merged_indices = []
    lc = 0

    # Read in input file
    with open(inp_file, 'r') as file:
        for line in file:
            new_line = []
            lc += 1

            # Find column indexes of start and stop
            if lc == 1:
                new_header, merged_indices = search_header(line.strip())
                continue

            split = line.strip().split(',')

            is_read = True
            first_pair = True
            last_index = -1

            for read_index, write_index in merged_indices:
                if first_pair:
                    first_pair = False
                    is_read = float(split[read_index]) != 0

                # Add prior indices
                for index in range(last_index + 1, read_index):
                    new_line.append(split[index])

                last_index = write_index

                if is_read:
                    new_line.append(split[read_index])
                else:
                    new_line.append(split[write_index])

            for index in range(last_index + 1, len(split)):
                new_line.append(split[index])

            new_lines.append(new_line)

    # Write output file
    with open(inp_file, 'w') as file:
        file.write(new_header + '\n')

        for line in new_lines:
            file.write(','.join(map(str, line)) + '\n')


def search_header(header: str):
    """Parses header columns ending in "-read" or "-write", yielding new header format and merged indices.

    Note this assumes if a column ending in "-read" exists, then a column ending in "-write" also exists right after it.

    :param header: The header to parse.
    :return: A tuple containing the (new header, a list of tuple pairs of indices to merge).
            ex. ('device,workload,throughput', (2, 3))
    """
    new_header = []
    merged_indices = []

    split = header.split(',')
    index = 0

    while index < len(split):
        if '-read' in split[index]:
            new_header.append(split[index].replace('-read', ''))
            merged_indices.append((index, index + 1))
            index += 1
        else:
            new_header.append(split[index])

        index += 1

    return ','.join(new_header), merged_indices


def usage():
    """Displays command-line information."""
    name = os.path.basename(__file__)
    print('%s %s' % (name, __version__))
    print('Usage: %s <inp-file>' % name)
    print('Command Line Arguments:')
    print('<inp-file>        : The iobs output file to modify.')
    print('Output: Aggregates read / write columns (such as throughput-read, throughput-write, etc.) into a single column.')


def main(argv: list):
    if '-h' in argv or '--help' in argv:
        usage()
        sys.exit(1)

    if len(argv) != 1:
        usage()
        sys.exit(1)

    inp_file = argv[0]

    if not os.path.isfile(inp_file):
        print('Input file given does not exist: %s' % inp_file)
        usage()
        sys.exit(1)

    aggregate_csv(inp_file)


if __name__ == '__main__':
    main(sys.argv[1:])
