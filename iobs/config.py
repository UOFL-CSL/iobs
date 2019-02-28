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


from abc import ABC, abstractmethod
from configparser import ConfigParser, ParsingError
import itertools
import json
import os

from iobs.errors import (
    ConfigNotFoundError,
    InvalidConfigError,
    InvalidSettingError,
    JobExecutionError,
    OutputFileError,
    OutputFormatError,
    OutputParsingError,
    RetryCountExceededError,
    UndefinedWorkloadTypeError
)
from iobs.output import printf, PrintType
from iobs.process import (
    change_scheduler,
    clear_caches,
    is_block_devices,
    run_command,
    validate_schedulers
)
from iobs.settings import (
    is_valid_workload_type,
    get_constant,
    get_formatter,
    match_regex,
    SettingAttribute,
    SettingsManager,
    validate_settings
)
from iobs.util import cast_bool, try_split


# region Config File
def parse_config_file(input_file):
    """Parses the supplied configuration file.

    Args:
        input_file: The file.

    Returns:
        A Configuration.

    Raises:
        InvalidConfigError: If unable to parse configuration.
    """
    printf('Parsing configuration file {}'.format(input_file),
           print_type=PrintType.DEBUG_LOG)

    if not os.path.isfile(input_file):
        raise ConfigNotFoundError('Config file {} not found'.format(input_file))

    config_parser = ConfigParser()

    try:
        config_parser.read(input_file, 'utf-8')
    except ParsingError as err:
        raise InvalidConfigError(
            'Invalid syntax in config file {}\n{}'.format(input_file, err)
        )

    global_header = get_constant('config_header_global')
    output_header = get_constant('config_header_output')
    template_header = get_constant('config_header_template')

    workload_type = get_workload_type(config_parser)
    workload_configuration_type = get_workload_configuration_type(workload_type)
    global_configuration_type = get_global_configuration_type(workload_type)
    output_configuration_type = get_output_configuration_type(workload_type)

    global_configuration = global_configuration_type()
    output_configuration = output_configuration_type(input_file)
    template_configuration = TemplateConfiguration()
    configuration = Configuration(
        input_file,
        workload_type,
        global_configuration,
        output_configuration,
        template_configuration
    )

    for section in config_parser.sections():
        if section == global_header:
            parse_section(config_parser, section, global_configuration)
        elif section == output_header:
            parse_section(config_parser, section, output_configuration)
        elif section == template_header:
            parse_section(config_parser, section, template_configuration)
        else:
            workload_configuration = workload_configuration_type(section)
            parse_section(config_parser, section, workload_configuration)
            configuration.add_workload_configuration(workload_configuration)

    printf('Configuration file {} parsed successfully'.format(input_file),
           print_type=PrintType.DEBUG_LOG)

    return configuration


def parse_section(config_parser, section, config):
    """Parses a section of a config file.

    Args:
        config_parser: The config parser.
        section: The section name.
        config: The Configuration object.
    """
    printf('Parsing section {}'.format(section),
           print_type=PrintType.DEBUG_LOG)

    for key, value in config_parser[section].items():
        config.add_setting(key, value)


def get_workload_type(config_parser):
    """Retrieves the workload type from the config file.

    Args:
        config_parser: The config parser.

    Returns:
        The workload type.

    Raises:
        InvalidConfigError: If workload type not in config or valid.
    """
    global_header = get_constant('config_header_global')
    if not config_parser.has_section(global_header):
        raise InvalidConfigError(
            'Missing required {} section in config'.format(global_header)
        )

    global_section = config_parser[global_header]
    if 'workload_type' not in global_section:
        raise InvalidConfigError('workload_type not defined in config')

    workload_type = global_section['workload_type']
    valid_workloads = get_constant('valid_workload_types')

    if workload_type not in valid_workloads:
        raise InvalidConfigError(
            'workload_type {} is not valid'.format(workload_type)
        )

    return workload_type
# endregion


# region Factories
def get_global_configuration_type(workload_type):
    if workload_type == 'fio':
        return FIOGlobalConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_job_type(workload_type):
    if workload_type == 'fio':
        return FIOJob

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_output_configuration_type(workload_type):
    if workload_type == 'fio':
        return FIOOutputConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_workload_configuration_type(workload_type):
    if workload_type == 'fio':
        return FIOWorkloadConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )
# endregion


# region Configuration
class Configuration:
    """A Configuration representing a single config input.

    Args:
        input_file: The input file.
        workload_type: The workload type.
        global_configuration: The GlobalConfiguration.
        output_configuration: The OutputConfiguration.
        template_configuration: The TemplateConfiguration.
    """
    def __init__(self, input_file, workload_type, global_configuration,
                 output_configuration, template_configuration):
        self._workload_type = workload_type
        self._input_file = input_file
        self._global_configuration = global_configuration
        self._output_configuration = output_configuration
        self._template_configuration = template_configuration
        self._workload_configurations = []

    def add_workload_configuration(self, workload_configuration):
        """Adds a WorkloadConfiguration to process.

        Args:
            workload_configuration: The workload configuration.
        """
        self._workload_configurations.append(workload_configuration)

    def process(self):
        """Processes the configuration."""
        printf('Processing input file {}'.format(self._input_file),
               print_type=PrintType.DEBUG_LOG)

        for wc in self._workload_configurations:
            wc.process(self._output_configuration, self._template_configuration,
                       self._global_configuration)

    def validate(self):
        """Validates the configuration."""
        printf('Validating input file {}'.format(self._input_file),
               print_type=PrintType.DEBUG_LOG)

        self._global_configuration.validate()
        self._output_configuration.validate()
        self._template_configuration.validate()

        for wc in self._workload_configurations:
            wc.validate()


class ConfigSectionBase(ABC):
    """Base class for configurations."""
    def __init__(self):
        self._settings = self._get_settings()

    @abstractmethod
    def add_setting(self, setting, value):
        """Adds a setting to the configuration object.

        Args:
            setting: The setting.
            value: The value.
        """

    @abstractmethod
    def _get_settings(self):
        """Retrieves the SettingAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to SettingAttributes.
        """

    def validate(self):
        """Validates the settings."""
        validate_settings(self, self._settings)


# region Global Configuration
class GlobalConfiguration(ConfigSectionBase):
    """Global Configuration for `global` section of config."""
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

    def _get_settings(self):
        """Retrieves the SettingAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to SettingAttributes.
        """
        return {
            'workload_type': SettingAttribute(
                validation_fn=is_valid_workload_type
            ),
            'devices': SettingAttribute(
                conversion_fn=try_split,
                validation_fn=is_block_devices
            ),
            'schedulers': SettingAttribute(
                conversion_fn=try_split,
                validation_fn=lambda x: validate_schedulers(x, self.devices),
                dependent_attributes=['devices']
            ),
            'repetitions': SettingAttribute(
                conversion_fn=int,
                validation_fn=lambda x: x >= 1,
                default_value=1
            )
        }


class FIOGlobalConfiguration(GlobalConfiguration):
    pass
# endregion


# region Workload Configuration
class WorkloadConfiguration(ConfigSectionBase):
    """Workload Configuration for `workload` sections of config.

    Args:
        name: The name of the workload.
    """
    def __init__(self, name):
        super().__init__()
        self._name = name

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

    def process(self, output_configuration, template_configuration,
                global_configuration):
        """Process the workload.

        Args:
            output_configuration: The OutputConfiguration.
            template_configuration: The TemplateConfiguration.
            global_configuration: The GlobalConfiguration.
        """
        printf('Processing workload {}'.format(self._name),
               print_type=PrintType.INFO_LOG)

        devices = global_configuration.devices
        schedulers = global_configuration.schedulers
        repetitions = global_configuration.repetitions
        job_type = get_job_type(global_configuration.workload_type)

        for device, scheduler in itertools.product(devices, schedulers):
            for file, sp in template_configuration.get_file_permutations(
                self.file, device, scheduler
            ):
                printf('Using template permutation {}'.format(sp),
                       print_type=PrintType.INFO_LOG)

                self.process_with_repetitions(output_configuration, file, device,
                                              scheduler, job_type, repetitions, sp)

    def process_with_repetitions(self, output_configuration, file, device,
                                 scheduler, job_type, repetitions,
                                 setting_permutation):
        """Process the workload for the template permutation and repetitions.

        Args:
            output_configuration: The OutputConfiguration.
            file: The input file.
            device: The device to execute on.
            scheduler: The schedulers to execute with.
            job_type: The job type.
            repetitions: The number of repetitions.
            setting_permutation: The template setting permutation.
        """
        for rep in range(repetitions):
            printf('Executing file {} with device {}, scheduler {}, repetition '
                   '{} of {}'.format(file, device, scheduler, rep + 1, repetitions),
                   print_type=PrintType.INFO_LOG)

            output = self._try_process(job_type, file, device, scheduler)
            output_configuration.process(output, setting_permutation,
                                         self._name, device, scheduler)

    def _try_process(self, job_type, file, device, scheduler):
        """Attempts to process a job with retrying if failure.

        Args:
            job_type: The job type.
            file: The input file.
            device: The device to execute on.
            scheduler: The scheduler to execute with.

        Returns:
            The output of processing the job.

        Raises:
            RetryCountExceededError: If job fails and retry counts are exceeded.
        """
        retry_count = SettingsManager.get('retry_count')

        for retry in range(retry_count):
            if retry != 0:
                printf('Retrying job...', print_type=PrintType.DEBUG_LOG)

            job = job_type(file, device, scheduler)

            try:
                return job.process()
            except JobExecutionError as err:
                printf('Unable to run job \n{}'.format(err),
                       print_type=PrintType.ERROR_LOG)

        raise RetryCountExceededError(
            'Unable to run job, exceeded retry counts'
        )

    def _get_settings(self):
        """Retrieves the SettingAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to SettingAttributes.
        """
        return {
            'file': SettingAttribute()
        }


class FIOWorkloadConfiguration(WorkloadConfiguration):
    pass
# endregion


class TemplateConfiguration(ConfigSectionBase):
    """Template Configuration for `template` section of config."""
    def __init__(self):
        super().__init__()
        self._dynamic_settings = set()

    def add_setting(self, setting, value):
        """Adds a setting to the configuration object.

        Args:
            setting: The setting.
            value: The value.
        """
        if setting in self._settings:
            sa = self._settings[setting]
            setattr(self, setting, sa.conversion_fn(value))

        else:
            setattr(self, setting, try_split(value, ','))
            self._dynamic_settings.add(setting)

    def get_file_permutations(self, file, device, scheduler):
        """Creates interpolated files of permutated template settings.

        Args:
            file: The input file.
            device: The device.
            scheduler: The scheduler.

        Returns:
            Yields tuples of file names and setting permutations.

        Raises:
            InvalidSettingError: If `file` does not exist.
        """
        if not os.path.isfile(file):
            raise InvalidSettingError(
                'Setting file {} does not exist'.format(file)
            )

        if self.enabled:
            for sp in self._get_setting_permutations():
                temp_file_name = self._interpolate_file(file, device, scheduler, sp)
                yield temp_file_name, sp
                os.remove(temp_file_name)
        else:
            yield file, ()

    def _get_setting_permutations(self):
        """Retrieves setting permutations.

        Returns:
            A list of setting permutation lists which are mappings of
            `setting_name=setting_value`.
        """
        setting_perm = []

        for setting in self._dynamic_settings:
            setting_perm.append([
                '{}={}'.format(setting, value)
                for value in getattr(self, setting)
            ])

        return itertools.product(*setting_perm)

    def _interpolate_file(self, file, device, scheduler, sp):
        """Creates a new file by reading and interpolating another.

        Args:
            file: The input file.
            device: The device.
            scheduler: The scheduler.
            sp: The permutated template settings.

        Returns:
            The name of the new file.
        """
        temp_file = file + '__temp__'
        with open(file, 'r') as inp:
            with open(temp_file, 'w') as out:
                for line in inp:
                    int_line = self._interpolate_text(
                        line, device, scheduler, sp
                    )
                    out.write(int_line)

        return temp_file

    def _interpolate_text(self, text, device, scheduler, sp):
        """Interpolates text.

        Args:
            text: The text to interpolate.
            device: The device.
            scheduler: The scheduler.
            sp: The permutated template settings.

        Returns:
            The interpolated text.
        """
        tf = get_formatter('template')
        device_name = match_regex(device, 'device_name')

        text = text.replace(tf.format('device'), device)
        text = text.replace(tf.format('device_name'), device_name)
        text = text.replace(tf.format('scheduler'), scheduler)

        for setting in sp:
            name, value = setting.split('=')
            text = text.replace(tf.format(name), value)

        return text

    def _get_settings(self):
        """Retrieves the SettingAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to SettingAttributes.
        """
        return {
            'enabled': SettingAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }


# region Output Configuration
class OutputConfiguration(ConfigSectionBase):
    """Output Configuration for `output` section of config.

    Args:
        input_file: The input file.
    """
    def __init__(self, input_file):
        super().__init__()
        self._input_file = input_file
        self._wrote_header = False

    @abstractmethod
    def _write_header(self, output, template_order,
                      setting_permutation_d, workload, device, scheduler):
        """Writes the header of the output file.

        Args:
            output: The job output.
            template_order: The ordered of setting permutations.
            setting_permutation_d: The setting permutation in dict form.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
        """

    @abstractmethod
    def _write_line(self, output, template_order,
                    setting_permutation_d, workload, device, scheduler):
        """Writes a line of the output file.

        Args:
            output: The job output.
            template_order: The ordered of setting permutations.
            setting_permutation_d: The setting permutation in dict form.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
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

    def process(self, output, setting_permutation, workload, device, scheduler):
        """Processes the output of a job.

        Args:
            output: The job output.
            setting_permutation: The template settings permutation.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
        """
        template_order = None
        setting_permutation_d = None

        if self.append_template:
            template_order = sorted([x.split('=')[0] for x in setting_permutation])
            setting_permutation_d = {}
            for sp in setting_permutation:
                k, v = sp.split('=')
                setting_permutation_d[k] = v

        if not self._wrote_header:
            self._write_header(output, template_order, setting_permutation_d,
                               workload, device, scheduler)
            self._wrote_header = True

        self._write_line(output, template_order, setting_permutation_d,
                         workload, device, scheduler)

    def _get_settings(self):
        """Retrieves the SettingAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to SettingAttributes.
        """
        return {
            'format': SettingAttribute(
                conversion_fn=try_split,
                default_value=self._get_default_format()
            ),
            'append_template': SettingAttribute(
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
            'include_lat_percentile': SettingAttribute(
                conversion_fn=cast_bool,
                default_value=False
            ),
            'include_clat_percentile': SettingAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }

    def _compare_percentile_format(self, setting_name, percentile_metric):
        pms = percentile_metric.split('-')
        return setting_name == '-'.join([pms[0], pms[1], pms[3]])

    def _write_header(self, output, template_order, setting_permutation_d,
                      workload, device, scheduler):
        """Writes the header of the output file.

        Args:
            output: The job output.
            template_order: The ordered of setting permutations.
            setting_permutation_d: The setting permutation in dict form.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
        """
        output_file = self.get_output_file()

        ft = self._get_format_translation()
        ut = self._get_universal_format_translation()
        lpt = self._get_lat_percentile_format_translation()
        cpt = self._get_clat_percentile_format_translation()
        po = self._get_percentile_order(output)

        with open(output_file, 'w+') as f:
            for fi in self.format:
                if fi in ft:
                    f.write(ft[fi])
                    f.write(',')
                elif fi in ut:
                    f.write(ut[fi])
                    f.write(',')
                elif fi in lpt:
                    if self.include_lat_percentile:
                        f.write(','.join(
                            p for p in po
                            if self._compare_percentile_format(fi, p))
                        )
                        f.write(',')
                elif fi in cpt:
                    if self.include_clat_percentile:
                        f.write(','.join(
                            p for p in po
                            if self._compare_percentile_format(fi, p))
                        )
                        f.write(',')

                else:
                    raise OutputFormatError(
                        'Output format is invalid, unable to parse {}'.format(fi)
                    )

            if self.append_template:
                for t in template_order:
                    f.write(t)
                    f.write(',')

            f.write('END\n')

    def _write_line(self, output, template_order, setting_permutation_d,
                    workload, device, scheduler):
        """Writes a line of the output file.

        Args:
            output: The job output.
            template_order: The ordered of setting permutations.
            setting_permutation_d: The setting permutation in dict form.
            workload: The workload name.
            device: The device.
            scheduler: The scheduler.
        """
        output_file = self.get_output_file()

        ft = self._get_format_translation()
        ut = self._get_universal_format_translation()
        lpt = self._get_lat_percentile_format_translation()
        cpt = self._get_clat_percentile_format_translation()
        po = self._get_percentile_order(output)

        with open(output_file, 'a') as f:
            for fi in self.format:
                if fi in ft:
                    f.write(str(output[ft[fi]]))
                    f.write(',')
                elif fi in ut:
                    if fi == 'workload':
                        f.write(workload)
                    elif fi == 'device':
                        f.write(device)
                    elif fi == 'scheduler':
                        f.write(scheduler)
                    else:
                        raise OutputFormatError(
                            'Unable to write metric {}'.format(fi)
                        )
                    f.write(',')
                elif fi in lpt:
                    if self.include_lat_percentile:
                        f.write(','.join(str(
                            output[p]) for p in po
                            if self._compare_percentile_format(fi, p))
                        )
                        f.write(',')
                elif fi in cpt:
                    if self.include_clat_percentile:
                        f.write(','.join(str(
                            output[p]) for p in po
                            if self._compare_percentile_format(fi, p))
                        )
                        f.write(',')
                else:
                    raise OutputFileError('Unable to write metric {}'.format(fi))

            if self.append_template:
                for t in template_order:
                    f.write(str(setting_permutation_d[t]))
                    f.write(',')

            f.write('END\n')
# endregion


# region Jobs
class Job(ABC):
    """A single unit of work to be executed.

    Args:
        file: The input file.
        device: The device.
        scheduler: The scheduler.
    """
    def __init__(self, file, device, scheduler):
        self.file = file
        self.device = device
        self.scheduler = scheduler

    def process(self):
        """Processes the job.

        Returns:
            The output of the job.
        """
        change_scheduler(self.device, self.scheduler)
        clear_caches(self.device)
        return self.execute()

    @abstractmethod
    def execute(self):
        """Executes the job."""


class FIOJob(Job):
    """An FIO Job."""
    def get_command(self):
        """Retrieves the command to execute.

        Returns:
            The command string.
        """
        return 'fio {} --output-format=json'.format(self.file)

    def collect_output(self, output):
        """Collects the output metrics from the job execution.

        Args:
            output: The raw output.

        Returns:
            A dictionary mapping metric names to values.

        Raises:
            OutputParsingError: If unable to parse raw output.
        """
        try:
            data = json.loads(output, encoding='utf-8')
            job_data = data['jobs'][0]

            metrics = {
                **self._parse_job_other(job_data),
                **self._parse_job_rw(job_data['read'], 'read'),
                **self._parse_job_rw(job_data['write'], 'write')
            }

            return metrics
        except (json.JSONDecodeError, KeyError, IndexError) as err:
            raise OutputParsingError(
                'Unable to parse output\n{}'.format(err)
            )

    def _parse_job_rw(self, data, rw):
        """Parses the job data from the raw output.

        Args:
            data: The data to parse.
            rw: Either 'read' or 'write'.

        Returns:
            A dictionary mapping the metric names to their values.
        """
        metrics = {
            'total-ios-{}'.format(rw): data['total_ios'],  # IO
            'io-kbytes-{}'.format(rw): data['io_kbytes'],  # KB
            'bw-{}'.format(rw): data['bw'],  # MB/s
            'iops-{}'.format(rw): data['iops'],  # IO/s
            'lat-min-{}'.format(rw): data['lat_ns']['min'],  # ns
            'lat-max-{}'.format(rw): data['lat_ns']['max'],  # ns
            'lat-mean-{}'.format(rw): data['lat_ns']['mean'],  # ns
            'lat-stddev-{}'.format(rw): data['lat_ns']['stddev'],  # ns
            'slat-min-{}'.format(rw): data['slat_ns']['min'],  # ns
            'slat-max-{}'.format(rw): data['slat_ns']['max'],  # ns
            'slat-mean-{}'.format(rw): data['slat_ns']['mean'],  # ns
            'slat-stddev-{}'.format(rw): data['slat_ns']['stddev'],  # ns
            'clat-min-{}'.format(rw): data['clat_ns']['min'],  # ns
            'clat-max-{}'.format(rw): data['clat_ns']['max'],  # ns
            'clat-mean-{}'.format(rw): data['clat_ns']['mean'],  # ns
            'clat-stddev-{}'.format(rw): data['clat_ns']['stddev']  # ns
        }

        if 'percentile' in data['lat_ns']:
            for p, v in data['lat_ns']['percentile'].items():
                metrics['lat-percentile-{}-{}'.format(p, rw)] = v  # ns

        if 'percentile' in data['clat_ns']:
            for p, v in data['clat_ns']['percentile'].items():
                metrics['clat-percentile-{}-{}'.format(p, rw)] = v  # ns

        return metrics

    def _parse_job_other(self, data):
        """Parses the other data from the raw output.

        Args:
            data: The data to parse.

        Returns:
            A dictionary mapping the metric names to their values.
        """
        return {
            'job-runtime': data['job_runtime']  # ms
        }

    def execute(self):
        """Executes the job.

        Returns:
            The collected output metrics.

        Raises JobExecutionError: If job failed to run.
        """
        command = self.get_command()
        out, _ = run_command(command)

        if out is None:
            raise JobExecutionError(
                'Unable to run command {} for device {}'
                .format(command, self.device)
            )

        printf('Job output:\n{}'.format(out), print_type=PrintType.DEBUG_LOG)

        return self.collect_output(out)
# endregion
# endregion
