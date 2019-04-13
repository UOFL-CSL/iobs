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


import itertools

from iobs.config.base import (
    ConfigAttribute,
    ConfigSectionBase
)
from iobs.errors import (
    InvalidSettingError,
    JobExecutionError,
    OutputParsingError,
    RetryCountExceededError
)
from iobs.output import printf, PrintType
from iobs.settings import SettingsManager


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

    def process(self, job_type, output_configuration, global_configuration,
                template_configuration, environment_configuration):
        """Process the workload.

        Args:
            job_type: The Job.
            output_configuration: The OutputConfiguration.
            template_configuration: The TemplateConfiguration.
            global_configuration: The GlobalConfiguration.
            environment_configuration: The EnvironmentConfiguration.
        """
        printf('Processing workload {}'.format(self._name),
               print_type=PrintType.INFO_LOG)

        devices = global_configuration.devices
        schedulers = global_configuration.schedulers
        repetitions = global_configuration.repetitions

        for device, scheduler in itertools.product(devices, schedulers):
            for file, tsp in template_configuration.get_file_permutations(
                self.file, device, scheduler
            ):
                printf('Using template permutation {}'.format(tsp),
                       print_type=PrintType.INFO_LOG)

                for esp in environment_configuration.get_environment_permutations(
                    device
                ):
                    printf('Using environment permutation {}'.format(esp),
                           print_type=PrintType.INFO_LOG)

                    self.process_with_repetitions(output_configuration, file,
                                                  device, scheduler, job_type,
                                                  repetitions, tsp, esp)

    def process_with_repetitions(self, output_configuration, file, device,
                                 scheduler, job_type, repetitions,
                                 template_setting_permutation,
                                 environment_setting_permutation):
        """Process the workload for the template permutation and repetitions.

        Args:
            output_configuration: The OutputConfiguration.
            file: The input file.
            device: The device to execute on.
            scheduler: The schedulers to execute with.
            job_type: The job type.
            repetitions: The number of repetitions.
            template_setting_permutation: The template setting permutation.
            environment_setting_permutation: The environment setting permutation.
        """
        for rep in range(repetitions):
            printf('Executing file {} with device {}, scheduler {}, repetition '
                   '{} of {}'.format(file, device, scheduler, rep + 1, repetitions),
                   print_type=PrintType.INFO_LOG)

            output = self._try_process(job_type, file, device, scheduler)
            output_configuration.process(output, self._name, device, scheduler,
                                         template_setting_permutation,
                                         environment_setting_permutation)

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
            except (JobExecutionError, OutputParsingError) as err:
                printf('Unable to run job \n{}'.format(err),
                       print_type=PrintType.ERROR_LOG)

        raise RetryCountExceededError(
            'Unable to run job, exceeded retry counts'
        )

    def _get_settings(self):
        """Retrieves the ConfigAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to ConfigAttributes.
        """
        return {
            'file': ConfigAttribute()
        }
