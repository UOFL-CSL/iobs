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


from abc import abstractmethod
import os

from iobs.config.base import (
    ConfigAttribute,
    ConfigSectionBase
)
from iobs.errors import (
    InvalidSettingError,
    OutputFileError,
    OutputFormatError
)
from iobs.settings import SettingsManager
from iobs.util import (
    cast_bool,
    try_split
)


class OutputConfiguration(ConfigSectionBase):
    """Output Configuration for `output` section of config.

    Args:
        input_file: The input file.
    """
    def __init__(self, input_file):
        super().__init__()
        self._input_file = input_file
        self._wrote_header = False
        self.header_order = []

    @abstractmethod
    def _write_header(self, output, universal_metrics,
                      template_order, template_spd,
                      environment_order, environment_spd):
        """Writes the header of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """

    @abstractmethod
    def _write_line(self, output,universal_metrics,
                    template_order, template_spd,
                    environment_order, environment_spd):
        """Writes a line of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """

    @abstractmethod
    def _get_default_format(self):
        """Retrieves the default format for the output if none is given.

        Returns:
            A list of string.
        """

    def add_setting(self, setting, value):
        """Adds a setting to the configuration object.

        Args:
            setting: The setting.
            value: The value.

        Raises:
            InvalidSettingError: If setting is not defined in `_get_settings`.
        """
        if setting not in self._settings:
            raise InvalidSettingError('Setting {} is not valid'.format(setting))

        sa = self._settings[setting]
        setattr(self, setting, sa.conversion_fn(value))

    def get_output_file(self):
        """Retrieves the output file name.

        Returns:
            The output file name.
        """
        output_base = os.path.basename(self._input_file)
        output_file = os.path.splitext(output_base)[0] + '.csv'
        output_directory = SettingsManager.get('output_directory')
        return os.path.join(output_directory, output_file)

    def process(self, output, workload, device, scheduler,
                template_setting_permutation, environment_setting_permutation):
        """Processes the output of a job.

        Args:
            output: The job output.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
            template_setting_permutation: The template settings permutation.
            environment_setting_permutation: The environment settings permutation.
        """
        template_order = None
        template_spd = None

        if self.append_template:
            template_order = self._get_permutation_order(template_setting_permutation)
            template_spd = self._get_permutation_setting_dict(template_setting_permutation)

        environment_order = None
        environment_spd = None

        if self.append_environment:
            environment_order = self._get_permutation_order(environment_setting_permutation)
            environment_spd = self._get_permutation_setting_dict(environment_setting_permutation)

        universal_metrics = {
            'workload': workload,
            'device': device,
            'scheduler': scheduler
        }
        if not self._wrote_header:
            self.header_order = self._write_header(
                output, universal_metrics, template_order,
                template_spd, environment_order, environment_spd
            )
            self._wrote_header = True

        self._write_line(output, universal_metrics,
                         template_order, template_spd,
                         environment_order, environment_spd)

    def _get_permutation_order(self, setting_permutation):
        """Retrieves a consistent ordering of setting permutation.

        Args:
            setting_permutation: The setting permutation.

        Returns:
            List of setting names.
        """
        return sorted([
            x.split('=')[0]
            for x in setting_permutation
        ])

    def _get_permutation_setting_dict(self, setting_permutation):
        """Converts a setting permutation into a dictionary mapping.

        Args:
            setting_permutation: The setting permutation.

        Returns:
            Dict mapping setting name to value.
        """
        spd = {}
        for sp in setting_permutation:
            k, v = sp.split('=')
            spd[k] = v
        return spd

    def _get_settings(self):
        """Retrieves the ConfigAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to ConfigAttributes.
        """
        return {
            'format': ConfigAttribute(
                conversion_fn=try_split,
                default_value=self._get_default_format()
            ),
            'append_template': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=True
            ),
            'append_environment': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=True
            )
        }

    def _get_universal_format_translation(self):
        """Retrieves universal format translation.

        Returns:
            A dictionary mapping formats to metrics.
        """
        f = {
            'w': 'workload',
            'd': 'device',
            's': 'scheduler',
        }

        f.update({x: x for x in f.values()})
        return f


class FilebenchOutputConfiguration(OutputConfiguration):
    def _get_default_format(self):
        """Retrieves the default format for the output if none is given.

        Returns:
            A list of string.
        """
        return [
            'workload',
            'device',
            'scheduler',
            'runtime',
            'total-ops',
            'throughput-ops',
            'read-throughput-ops',
            'write-throughput-ops',
            'throughput-mb',
            'average-lat',
            'flowops'
        ]

    def _get_format_translation(self):
        """Retrieves format translation.

        Returns:
            A dictionary mapping formats to metrics.
        """
        f = {
            'run': 'runtime',
            'ops': 'total-ops',
            'top': 'throughput-ops',
            'rto': 'read-throughput-ops',
            'wto': 'write-throughput-ops',
            'tmb': 'throughput-mb',
            'avl': 'average-lat'
        }

        f.update({x: x for x in f.values()})
        return f

    def _get_flowops_order(self, output):
        """Returns a list of percentile in ascending order.

        Args:
            output: The job output.

        Returns:
            A list of strings.
        """
        ft = self._get_format_translation()
        return sorted([x for x in output if x not in ft])

    def _get_settings(self):
        return {
            **super()._get_settings(),
            'include_flowops': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }

    def _write_header(self, output, universal_metrics,
                      template_order, template_spd,
                      environment_order, environment_spd):
        """Writes the header of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """
        header_order = []
        output_file = self.get_output_file()

        ft = self._get_format_translation()
        ut = self._get_universal_format_translation()
        fo = self._get_flowops_order(output)

        with open(output_file, 'w+') as f:
            for fi in self.format:
                if fi in ft:
                    header_order.append(ft[fi])
                    f.write(ft[fi])
                    f.write(',')
                elif fi in ut:
                    header_order.append(ut[fi])
                    f.write(ut[fi])
                    f.write(',')
                elif fi in output:
                    header_order.append(fi)
                    f.write(fi)
                    f.write(',')
                elif fi == 'flowops':
                    if self.include_flowops:
                        header_order.extend(fo)
                        f.write(','.join(fo))
                        f.write(',')
                elif fi in template_spd:
                    header_order.append(fi)
                    f.write(fi)
                    f.write(',')
                elif fi in environment_spd:
                    header_order.append(fi)
                    f.write(fi)
                    f.write(',')
                else:
                    raise OutputFormatError(
                        'Output format is invalid, unable to parse {}'.format(fi)
                    )

            if self.append_template:
                for t in template_order:
                    header_order.append(t)
                    f.write(t)
                    f.write(',')

            if self.append_environment:
                for t in environment_order:
                    header_order.append(t)
                    f.write(t)
                    f.write(',')

            f.write('END\n')

        return header_order

    def _write_line(self, output, universal_metrics,
                    template_order, template_spd,
                    environment_order, environment_spd):
        """Writes a line of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """
        output_file = self.get_output_file()

        with open(output_file, 'a') as f:
            for fi in self.header_order:
                if fi in output:
                    f.write(str(output[fi]))
                    f.write(',')
                elif fi in universal_metrics:
                    f.write(str(universal_metrics[fi]))
                    f.write(',')
                elif fi in template_spd:
                    f.write(str(template_spd[fi]))
                    f.write(',')
                elif fi in environment_spd:
                    f.write(str(environment_spd[fi]))
                    f.write(',')
                else:
                    raise OutputFormatError('Unable to write metric {}'.format(fi))

            f.write('END\n')


class FIOOutputConfiguration(OutputConfiguration):
    def _get_default_format(self):
        """Retrieves the default format for the output if none is given.

        Returns:
            A list of string.
        """
        return [
            'workload',
            'device',
            'scheduler',
            'job-runtime',
            'total-ios-read',
            'total-ios-write',
            'io-kbytes-read',
            'io-kbytes-write',
            'bw-read',
            'bw-write',
            'iops-read',
            'iops-write',
            'lat-min-read',
            'lat-min-write',
            'lat-max-read',
            'lat-max-write',
            'lat-mean-read',
            'lat-mean-write',
            'lat-stddev-read',
            'lat-stddev-write',
            'slat-min-read',
            'slat-min-write',
            'slat-max-read',
            'slat-max-write',
            'slat-mean-read',
            'slat-mean-write',
            'slat-stddev-read',
            'slat-stddev-write',
            'clat-min-read',
            'clat-min-write',
            'clat-max-read',
            'clat-max-write',
            'clat-mean-read',
            'clat-mean-write',
            'clat-stddev-read',
            'clat-stddev-write',
            'clat-percentile-read',
            'clat-percentile-write'
        ]

    def _get_format_translation(self):
        """Retrieves format translation.

        Returns:
            A dictionary mapping formats to metrics.
        """
        f = {
            'run': 'job-runtime',
            'tir': 'total-ios-read',
            'tiw': 'total-ios-write',
            'ibr': 'io-kbytes-read',
            'ibw': 'io-kbytes-write',
            'bwr': 'bw-read',
            'bww': 'bw-write',
            'opr': 'iops-read',
            'ipw': 'iops-write',
            'lir': 'lat-min-read',
            'liw': 'lat-min-write',
            'lar': 'lat-max-read',
            'law': 'lat-max-write',
            'lmr': 'lat-mean-read',
            'lmw': 'lat-mean-write',
            'lsr': 'lat-stddev-read',
            'lsw': 'lat-stddev-write',
            'sir': 'slat-min-read',
            'siw': 'slat-min-write',
            'sar': 'slat-max-read',
            'saw': 'slat-max-write',
            'smr': 'slat-mean-read',
            'smw': 'slat-mean-write',
            'ssr': 'slat-stddev-read',
            'ssw': 'slat-stddev-write',
            'cir': 'clat-min-read',
            'ciw': 'clat-min-write',
            'car': 'clat-max-read',
            'caw': 'clat-max-write',
            'cmr': 'clat-mean-read',
            'cmw': 'clat-mean-write',
            'csr': 'clat-stddev-read',
            'csw': 'clat-stddev-write'
        }

        f.update({x: x for x in f.values()})
        return f

    def _get_clat_percentile_format_translation(self):
        """Retrieves percentile format translation.

        Returns:
            A dictionary mapping formats to metrics.
        """
        f = {
            'cpr': 'clat-percentile-read',
            'cpw': 'clat-percentile-write',
        }

        f.update({x: x for x in f.values()})
        return f

    def _get_lat_percentile_format_translation(self):
        """Retrieves percentile format translation.

        Returns:
            A dictionary mapping formats to metrics.
        """
        f = {
            'lpr': 'lat-percentile-read',
            'lpw': 'lat-percentile-write'
        }

        f.update({x: x for x in f.values()})
        return f

    def _get_percentile_order(self, output):
        """Returns a list of percentile in ascending order.

        Args:
            output: The job output.

        Returns:
            A list of strings.
        """
        return sorted([
            x for x in output if 'percentile' in x
        ], key=lambda x: float(x.split('-')[-2]))

    def _get_settings(self):
        return {
            **super()._get_settings(),
            'include_lat_percentile': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            ),
            'include_clat_percentile': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }

    def _compare_percentile_format(self, setting_name, percentile_metric):
        pms = percentile_metric.split('-')
        return setting_name == '-'.join([pms[0], pms[1], pms[3]])

    def _write_header(self, output, universal_metrics,
                      template_order, template_spd,
                      environment_order, environment_spd):
        """Writes the header of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """
        header_order = []
        output_file = self.get_output_file()

        ft = self._get_format_translation()
        ut = self._get_universal_format_translation()
        lpt = self._get_lat_percentile_format_translation()
        cpt = self._get_clat_percentile_format_translation()
        po = self._get_percentile_order(output)

        with open(output_file, 'w+') as f:
            for fi in self.format:
                if fi in ft:
                    header_order.append(ft[fi])
                    f.write(ft[fi])
                    f.write(',')
                elif fi in ut:
                    header_order.append(ut[fi])
                    f.write(ut[fi])
                    f.write(',')
                elif fi in lpt:
                    if self.include_lat_percentile:
                        lp = [p for p in po
                              if self._compare_percentile_format(fi, p)]
                        header_order.extend(lp)
                        f.write(','.join(lp))
                        f.write(',')
                elif fi in cpt:
                    if self.include_clat_percentile:
                        cp = [p for p in po
                              if self._compare_percentile_format(fi, p)]
                        header_order.extend(cp)
                        f.write(','.join(cp))
                        f.write(',')
                elif fi in template_spd:
                    header_order.append(fi)
                    f.write(fi)
                    f.write(',')
                elif fi in environment_spd:
                    header_order.append(fi)
                    f.write(fi)
                    f.write(',')
                else:
                    raise OutputFormatError(
                        'Output format is invalid, unable to parse {}'.format(fi)
                    )

            if self.append_template:
                for t in template_order:
                    header_order.append(t)
                    f.write(t)
                    f.write(',')

            if self.append_environment:
                for t in environment_order:
                    header_order.append(t)
                    f.write(t)
                    f.write(',')

            f.write('END\n')

        return header_order

    def _write_line(self, output, universal_metrics,
                    template_order, template_spd,
                    environment_order, environment_spd):
        """Writes a line of the output file.

        Args:
            output: The job output.
            universal_metrics: A dictionary of universal metrics.
            template_order: The ordered of template setting permutations.
            template_spd: The template setting permutation in dict form.
            environment_order: The ordered of environment setting permutations.
            environment_spd: The environment setting permutation in dict form.
        """
        output_file = self.get_output_file()

        with open(output_file, 'a') as f:
            for fi in self.header_order:
                if fi in output:
                    f.write(str(output[fi]))
                    f.write(',')
                elif fi in universal_metrics:
                    f.write(str(universal_metrics[fi]))
                    f.write(',')
                elif fi in template_spd:
                    f.write(str(template_spd[fi]))
                    f.write(',')
                elif fi in environment_spd:
                    f.write(str(environment_spd[fi]))
                    f.write(',')
                else:
                    raise OutputFormatError('Unable to write metric {}'.format(fi))

            f.write('END\n')
