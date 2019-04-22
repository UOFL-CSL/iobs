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

from iobs.errors import InvalidSettingError
from iobs.output import printf, PrintType
from iobs.process import (
    change_nomerges,
    change_randomize_va_space,
    change_scheduler,
    get_device_nomerges,
    get_randomize_va_space,
    get_device_scheduler
)


class ConfigAttribute:
    """Attribute properties for configurations.

    Args:
        conversion_fn: Function to convert the string representation into
            another type.
        validation_fn: Function to validate the value of the setting.
        dependent_attributes: Other attributes which this is dependent on.
        default_value: Default value if none explicitly assigned.
    """
    def __init__(self, conversion_fn=lambda x: str(x),
                 validation_fn=lambda x: True,
                 dependent_attributes=None,
                 default_value=None):
        self.conversion_fn = conversion_fn
        self.validation_fn = validation_fn
        self.dependent_attributes = dependent_attributes
        self.default_value = default_value
        self.default_used = False


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
        """Validates the settings.

        Raises:
            InvalidSettingError: If settings are not valid or dependencies not met.
        """
        # NOTE: Assumes that there are no circular dependencies
        roots = {
            k for k, v in self._settings.items()
            if not v.dependent_attributes
        }
        unvalidated = {
            k for k, v in self._settings.items()
            if v.dependent_attributes
        }
        inc_deps = {s: set() for s in self._settings}
        out_deps = {
            k: set(v.dependent_attributes)
            for k, v in self._settings.items()
            if v.dependent_attributes
        }

        for k, v in self._settings.items():
            if v.dependent_attributes:
                for dep in v.dependent_attributes:
                    inc_deps[dep].add(k)

        while roots:
            setting_name = roots.pop()
            setting = self._settings[setting_name]
            self._validate_setting(setting_name, setting)

            for dep in inc_deps[setting_name]:
                out_deps[dep].remove(setting_name)
                if not out_deps[dep]:
                    del out_deps[dep]
                    unvalidated.remove(dep)
                    roots.add(dep)

        if unvalidated:
            raise InvalidSettingError(
                'Setting(s) {} do not have dependencies met'
                .format(', '.join(unvalidated))
            )

    def _validate_setting(self, setting_name, setting):
        """Validates a config setting attribute.

        Args:
            setting_name: The attribute.
            setting: The SettingAttribute.

        Raises:
            InvalidSettingError: If no value set and `default_value` not set on
                `setting`. Or if fails `validate_fn` on `setting`.
        """
        setting_value = getattr(self, setting_name, None)
        if setting_value is None:
            if setting.default_value is None:
                raise InvalidSettingError(
                    'Required setting {} is not defined'.format(setting_name)
                )

            setting_value = setting.default_value
            setting.default_used = True
            setattr(self, setting_name, setting.default_value)

        if not setting.validation_fn(setting_value):
            raise InvalidSettingError(
                'Setting {}={} is not valid'.format(setting_name, setting_value)
            )


class Configuration:
    """A Configuration representing a single config input.

    Args:
        input_file: The input file.
        workload_type: The workload type.
        global_configuration: The GlobalConfiguration.
        output_configuration: The OutputConfiguration.
        template_configuration: The TemplateConfiguration.
    """
    def __init__(self, input_file, workload_type, job_type,
                 global_configuration, output_configuration,
                 template_configuration, environment_configuration):
        self._input_file = input_file
        self._workload_type = workload_type
        self._job_type = job_type
        self._global_configuration = global_configuration
        self._output_configuration = output_configuration
        self._template_configuration = template_configuration
        self._environment_configuration = environment_configuration
        self._workload_configurations = []
        self._device_environments = {}

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

        if self._workload_type == 'filebench':
            change_randomize_va_space(0)

        for wc in self._workload_configurations:
            wc.process(
                self._job_type,
                self._output_configuration,
                self._global_configuration,
                self._template_configuration,
                self._environment_configuration
            )

    def restore_device_environments(self):
        """Restores device environments.

        NOTE: This should be called after `save_device_environments` has been
        called.
        """
        printf('Restoring device information...',
               print_type=PrintType.DEBUG_LOG)

        for device in self._global_configuration.devices:
            if device not in self._device_environments:
                continue

            de = self._device_environments[device]

            printf('Restoring device {} environment: {}'.format(device, de),
                   print_type=PrintType.DEBUG_LOG)

            change_nomerges(device, de['nomerges'])
            change_scheduler(device, de['scheduler'])

    def save_device_environments(self):
        """Saves device environment information so it can be restored.

        NOTE: This should be called after `validate` has bee called.
        """
        printf('Saving device information...',
               print_type=PrintType.DEBUG_LOG)

        for device in self._global_configuration.devices:
            self._device_environments[device] = {
                'nomerges': get_device_nomerges(device),
                'scheduler': get_device_scheduler(device)
            }

            de = self._device_environments[device]

            printf('Saving device {} environment: {}'.format(device, de),
                   print_type=PrintType.DEBUG_LOG)

    def validate(self):
        """Validates the configuration."""
        printf('Validating input file {}'.format(self._input_file),
               print_type=PrintType.DEBUG_LOG)

        self._environment_configuration.validate()
        self._global_configuration.validate()
        self._output_configuration.validate()
        self._template_configuration.validate()

        for wc in self._workload_configurations:
            wc.validate()
