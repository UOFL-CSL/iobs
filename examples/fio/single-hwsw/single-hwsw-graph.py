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
import sys

from cycler import cycler
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
import matplotlib.ticker as plticker


def parse_input(args):
    header_read = False
    header = {}
    metrics = {}

    with open(args.input, 'r') as f:
        for line in f:
            if not header_read:
                header = read_header(line)
                header_read = True
            else:
                read_line(line, header, metrics)

    return average_metrics(metrics)


def read_header(line):
    return {v: i for i, v in enumerate(line.split(','))}


def read_line(line, header, metrics):
    line_split = line.split(',')
    device = line_split[header['device']]
    scheduler = line_split[header['scheduler']]
    lat = float(line_split[header['lat-mean-read']])
    d2c = float(line_split[header['d2c-avg']]) * 10**9

    if device not in metrics:
        metrics[device] = {}

    if scheduler not in metrics[device]:
        metrics[device][scheduler] = {
            'count': 0,
            'lats': [],
            'd2cs': []
        }

    m = metrics[device][scheduler]
    m['count'] += 1
    m['lats'].append(lat)
    m['d2cs'].append(d2c)


def average_metrics(metrics):
    for device in metrics:
        for scheduler in metrics[device]:
            m = metrics[device][scheduler]
            total = sum(m['lats']) / m['count']
            hw = sum(m['d2cs']) / m['count']
            sw = total - hw

            # Relative percentages
            m['a'] = total
            m['b'] = hw
            m['sw'] = sw / total * 100
            del m['count']
            del m['lats']
            del m['d2cs']
    return metrics


def get_device_name(args, device):
    for s in args.device_names:
        d, n = s.split('=')
        if d == device:
            return n
    raise ValueError('Device not listed in --device-names')


def count_devices(metrics):
    return len(metrics)


def count_schedulers(metrics):
    for devices in metrics:
        return len(metrics[devices])


def get_color_cycler(metrics):
    num_devices = count_devices(metrics)
    num_schedulers = count_schedulers(metrics)
    colors = ['b', 'g', 'r', 'y', 'm', 'c', 'k'][:num_schedulers]
    line_styles = ['-', '--', ':', '-.'][:num_devices]
    return cycler('linestyle', line_styles) * cycler('color', colors)


def get_plot_labels():
    for c in 'abcdefghijklmnopqrstuvwxyz':
        yield c


def plot_graphs(args, metrics):
    color_cycler = get_color_cycler(metrics)
    num_devices = count_devices(metrics)
    num_schedulers = count_schedulers(metrics)

    plt.rc('axes', prop_cycle=color_cycler)

    font_props = FontProperties()
    font_props.set_size('small')

    output_base = os.path.basename(args.input)
    output_file = os.path.splitext(output_base)[0]

    shrink_x, shrink_y = 0, 0.1

    with PdfPages(output_file + '.latency.pdf') as pdf:
        # Average Latency
        gpl = iter(get_plot_labels())

        # Adjust dimensions
        figure, axes = plt.subplots(2, 2, sharex='all')
        figure.subplots_adjust(top=0.92, bottom=0.2)

        for di, device in enumerate(sorted(metrics)):
            device_name = get_device_name(args, device)
            ax = axes[di // 2, di % 2]
            x = list(range(num_schedulers))
            y_hw = []

            for si, scheduler in enumerate(sorted(metrics[device])):
                m = metrics[device][scheduler]
                y_hw.append(m['sw'])

            ax.bar(x, y_hw, label='SW (Application + Kernel)', color='gray')

            # Shrink axis
            box = ax.get_position()
            ax.set_position([box.x0 + box.width * shrink_x,
                             box.y0 + box.height * shrink_y,
                             box.width * (1 - shrink_x),
                             box.height * (1 - shrink_y)])

            if di // 2 == 0:
                ax.set_title('({}) {}'.format(next(gpl), device_name), y=-0.3)
            else:
                ax.set_title('({}) {}'.format(next(gpl), device_name), y=-0.65)

            # Set tick intervals
            ax.set_ylim(bottom=0, top=100)

            # Set Labels
            ax.set_ylabel('Latency (%)')

            ax.set_xticks(list(range(num_schedulers)))
            ax.set_xticklabels(list(sorted(metrics[device])), rotation=45, fontsize='x-small')

        # Add Legend
        handles, labels = ax.get_legend_handles_labels()
        figure.legend(handles, labels, loc='upper center', fancybox=True,
                      shadow=True, ncol=num_schedulers, prop=font_props)

        figure.savefig(output_file + '.eps')
        pdf.savefig(figure)


def main(argv):
    parser = argparse.ArgumentParser(prog='hwsw-graph')
    parser.add_argument(
        'input',
        help='The csv input file.'
    )
    parser.add_argument(
        '-d', '--device_names',
        nargs='+',
        required=True,
        help='Device name mapping of the following format: '
             'device1=device-name1 device2=device-name2 ...'
    )
    args = parser.parse_args(argv)
    metrics = parse_input(args)
    plot_graphs(args, metrics)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
