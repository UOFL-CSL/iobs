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
    rwmixwrite = int(line_split[header['rwmixwrite']])
    clat_mean_read = float(line_split[header['clat-mean-read']])
    clat_percentile_99999000_read = float(line_split[header['clat-percentile-99.999000-read']])

    if device not in metrics:
        metrics[device] = {}

    if scheduler not in metrics[device]:
        metrics[device][scheduler] = {}

    if rwmixwrite not in metrics[device][scheduler]:
        metrics[device][scheduler][rwmixwrite] = {
            'count': 0,
            'clat_mean_read': 0,
            'clat_percentile_99999000_read': 0
        }

    m = metrics[device][scheduler][rwmixwrite]
    m['count'] += 1
    m['clat_mean_read'] += clat_mean_read
    m['clat_percentile_99999000_read'] += clat_percentile_99999000_read


def average_metrics(metrics):
    for device in metrics:
        for scheduler in metrics[device]:
            for rwmixwrite in metrics[device][scheduler]:
                m = metrics[device][scheduler][rwmixwrite]
                m['clat_mean_read'] /= m['count']
                m['clat_percentile_99999000_read'] /= m['count']
                del m['count']

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

    figure, axes = plt.subplots(2, num_devices, sharex='all')
    plt.rc('axes', prop_cycle=color_cycler)

    font_props = FontProperties()
    font_props.set_size('small')

    output_base = os.path.basename(args.input)
    output_file = os.path.splitext(output_base)[0]
    gpl = iter(get_plot_labels())

    shrink_x, shrink_y = 0, 0.2

    # Adjust dimensions
    figure.subplots_adjust(top=0.92, bottom=0.2)

    xloc = plticker.MultipleLocator(base=10.0)
    yloc = plticker.MultipleLocator(base=500.0)

    # Average latency
    for di, device in enumerate(sorted(metrics)):
        device_name = get_device_name(args, device)

        ax = axes[0, di]

        for scheduler in sorted(metrics[device]):
            if scheduler not in args.schedulers: continue
            m = metrics[device][scheduler]
            x = sorted([w for w in m])
            y = [m[x]['clat_mean_read'] / 10**3 for x in sorted(m)]
            ax.plot(x, y, label=scheduler)

        # Shrink axis
        box = ax.get_position()
        ax.set_position([box.x0 + box.width * shrink_x,
                         box.y0 + box.height * shrink_y,
                         box.width * (1 - shrink_x),
                         box.height * (1 - shrink_y)])

        ax.set_title('({}) {} average'.format(next(gpl), device_name), y=-0.35)

        # Set tick intervals
        ax.xaxis.set_major_locator(xloc)
        # ax.yaxis.set_major_locator(yloc)
        ax.set_ylim(bottom=0)

        # Set Labels
        if di == 0:
            ax.set_ylabel('Read Latency (μs)')

    # 99.999th latency
    for di, device in enumerate(sorted(metrics)):
        device_name = get_device_name(args, device)

        ax = axes[1, di]

        for scheduler in sorted(metrics[device]):
            if scheduler not in args.schedulers: continue
            m = metrics[device][scheduler]
            x = sorted([w for w in m])
            y = [m[x]['clat_percentile_99999000_read'] / 10**6 for x in sorted(m)]
            ax.plot(x, y, label=scheduler)

        # Shrink axis
        box = ax.get_position()
        ax.set_position([box.x0 + box.width * shrink_x,
                         box.y0 + box.height * shrink_y,
                         box.width * (1 - shrink_x),
                         box.height * (1 - shrink_y)])

        ax.set_title('({}) {} 99.999th'.format(next(gpl), device_name), y=-0.65)

        # Set Labels
        if di == 0:
            ax.set_ylabel('Read Latency (ms)')
        ax.set_xlabel('Write fraction (%)')

        # Set tick intervals
        ax.xaxis.set_major_locator(xloc)
        # ax.yaxis.set_major_locator(yloc)
        ax.set_ylim(bottom=0)

    # Add Legend
    handles, labels = ax.get_legend_handles_labels()
    figure.legend(handles, labels, loc='upper center', fancybox=True,
                  shadow=True, ncol=num_schedulers, prop=font_props)

    # Align y-labels
    figure.align_ylabels(axes[:, 0])

    figure.savefig(output_file + '.eps')

    with PdfPages(output_file + '.pdf') as pdf:
        pdf.savefig(figure)


def main(argv):
    parser = argparse.ArgumentParser(prog='mixed-percentage-graph')
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
    parser.add_argument(
        '-s', '--schedulers',
        nargs='+',
        required=True,
        help='Schedulers to include in output.'
    )
    args = parser.parse_args(argv)
    metrics = parse_input(args)
    plot_graphs(args, metrics)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
