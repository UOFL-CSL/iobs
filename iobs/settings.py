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
    'config_header_environment': 'environment',
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
