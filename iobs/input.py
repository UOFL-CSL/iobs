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


from configparser import ConfigParser, ParsingError
import os

from iobs.config.base import Configuration
from iobs.config.factory import (
    get_environment_configuration_type,
    get_global_configuration_type,
    get_job_type,
    get_output_configuration_type,
    get_template_configuration_type,
    get_workload_configuration_type
)
from iobs.errors import (
    ConfigNotFoundError,
    InvalidConfigError
)
from iobs.output import printf, PrintType
from iobs.settings import get_constant


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

    environment_header = get_constant('config_header_environment')
    global_header = get_constant('config_header_global')
    output_header = get_constant('config_header_output')
    template_header = get_constant('config_header_template')

    workload_type = get_workload_type(config_parser)
    environment_configuration_type = get_environment_configuration_type(workload_type)
    global_configuration_type = get_global_configuration_type(workload_type)
    output_configuration_type = get_output_configuration_type(workload_type)
    template_configuration_type = get_template_configuration_type(workload_type)
    workload_configuration_type = get_workload_configuration_type(workload_type)
    job_type = get_job_type(workload_type)

    environment_configuration = environment_configuration_type()
    global_configuration = global_configuration_type()
    output_configuration = output_configuration_type(input_file)
    template_configuration = template_configuration_type()
    configuration = Configuration(
        input_file,
        workload_type,
        job_type,
        global_configuration,
        output_configuration,
        template_configuration,
        environment_configuration
    )

    for section in config_parser.sections():
        if section == environment_header:
            parse_section(config_parser, section, environment_configuration)
        elif section == global_header:
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
