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


import os
import re

from iobs.errors import (
    InvalidSettingError,
    UndefinedConstantError,
    UndefinedFormatterError,
    UndefinedRegexError
)


_CONSTANTS = {
    'config_header_global': 'global',
    'config_header_output': 'output',
    'config_header_template': 'template',
    'valid_workload_types': {'fio'}
}

_FORMATTERS = {
    'template': '<%{}%>'
}


_REGEX = {
    'device_name': re.compile(r'/dev/(.*)')
}


class SettingsManager:
    """Controls settings set by command-line arguments."""
    continue_on_failure = False
    log_enabled = False
    output_directory = os.getcwd()
    retry_count = 1
    silent = False

    @staticmethod
    def get(*settings):
        """Retrieves attributes on self.

        Args:
            settings: The attributes to retrieve.

        Returns:
            The values of the attributes on self.

        Raises:
            InvalidSettingError: If setting does not exist on self.
        """
        ret = []
        for setting in settings:
            try:
                ret.append(getattr(SettingsManager, setting))
            except AttributeError:
                raise InvalidSettingError('{} does not exist'.format(setting))

        if len(ret) == 1:
            return ret[0]
        return ret

    @staticmethod
    def set(setting, value):
        """Sets an attribute on self.

        Args:
            setting: The attribute to set.
            value: The value to set the attribute to.
        """
        setattr(SettingsManager, setting, value)


class SettingAttribute:
    """Attribute properties for settings.

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


def get_constant(name):
    """Retrieves a constant.

    Args:
        name: The name of the constant.

    Returns:
        The constant.

    Raises:
        UndefinedConstantError: If constant is not defined.
    """
    if name not in _CONSTANTS:
        raise UndefinedConstantError(
            'Constant {} is not defined'.format(name)
        )

    return _CONSTANTS[name]


def get_formatter(name):
    """Retrieves a formatter.

    Args:
        name: The name of the formatter.

    Returns:
        The formatter.

    Raises:
        UndefinedFormatterError: If formatter is not defined.
    """
    if name not in _FORMATTERS:
        raise UndefinedFormatterError(
            'Formatter {} is not defined'.format(name)
        )

    return _FORMATTERS[name]


def is_valid_workload_type(workload_type):
    """Validates a given workload type.

    Args:
        workload_type: The workload type.

    Returns:
        True if value, else False.
    """
    return workload_type in get_constant('valid_workload_types')


def match_regex(string, regex_name):
    """Returns the matching regex pattern in the string.

    Args:
        string: The string to search.
        regex_name: The name of the regex to match on.

    Returns:
        Regex match or None if there isn't a match.

    Raises:
        UndefinedRegexError: If `regex_name` isn't a defined regex.
    """
    if regex_name not in _REGEX:
        raise UndefinedRegexError('regex {} is not defined'.format(regex_name))

    regex = _REGEX[regex_name]
    match = regex.match(string)

    if not match:
        return None

    return match[1]


def validate_setting(obj, setting_name, setting):
    """Validates the setting attribute on an object.

    Args:
        obj: The object.
        setting_name: The attribute.
        setting: The SettingAttribute.

    Raises:
        InvalidSettingError: If no value set and `default_value` not set on
            `setting`. Or if fails `validate_fn` on `setting`.
    """
    setting_value = getattr(obj, setting_name, None)
    if setting_value is None:
        if setting.default_value is None:
            raise InvalidSettingError(
                'Required setting {} is not defined'.format(setting_name)
            )

        setting_value = setting.default_value
        setattr(obj, setting_name, setting.default_value)

    if not setting.validation_fn(setting_value):
        raise InvalidSettingError(
            'Setting {}={} is not valid'.format(setting_name, setting_value)
        )


def validate_settings(obj, settings):
    """Validates the settings on an object.

    Args:
        obj: An object to check attributes of.
        settings: A dictionary mapping names to SettingAttributes.

    Raises:
        InvalidSettingError: If settings are not valid or dependencies not met.
    """
    # NOTE: Assumes that there are no circular dependencies
    roots = {
        k for k, v in settings.items()
        if not v.dependent_attributes
    }
    unvalidated = {
        k for k, v in settings.items()
        if v.dependent_attributes
    }
    inc_deps = {s: set() for s in settings}
    out_deps = {
        k: set(v.dependent_attributes)
        for k, v in settings.items()
        if v.dependent_attributes
    }

    for k, v in settings.items():
        if v.dependent_attributes:
            for dep in v.dependent_attributes:
                inc_deps[dep].add(k)

    while roots:
        setting_name = roots.pop()
        setting = settings[setting_name]
        validate_setting(obj, setting_name, setting)

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
