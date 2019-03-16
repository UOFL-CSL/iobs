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


from iobs.config.jobs import FIOJob
from iobs.config.outputs import FIOOutputConfiguration
from iobs.config.environments import EnvironmentConfiguration
from iobs.config.globals import GlobalConfiguration
from iobs.config.templates import TemplateConfiguration
from iobs.config.workloads import WorkloadConfiguration
from iobs.errors import UndefinedWorkloadTypeError
from iobs.settings import get_constant


def get_global_configuration_type(workload_type):
    valid_workload_types = get_constant('valid_workload_types')

    if workload_type in valid_workload_types:
        return GlobalConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_job_type(workload_type):
    if workload_type == 'fio':
        return FIOJob

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_environment_configuration_type(workload_type):
    valid_workload_types = get_constant('valid_workload_types')

    if workload_type in valid_workload_types:
        return EnvironmentConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_output_configuration_type(workload_type):
    if workload_type == 'fio':
        return FIOOutputConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_template_configuration_type(workload_type):
    valid_workload_types = get_constant('valid_workload_types')

    if workload_type in valid_workload_types:
        return TemplateConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )


def get_workload_configuration_type(workload_type):
    if workload_type == 'fio':
        return FIOWorkloadConfiguration

    raise UndefinedWorkloadTypeError(
        'workload_type {} is not defined'.format(workload_type)
    )
