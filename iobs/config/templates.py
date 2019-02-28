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
import os

from iobs.config.base import (
    ConfigAttribute,
    ConfigSectionBase
)
from iobs.errors import (
    InvalidSettingError
)
from iobs.settings import (
    get_formatter,
    match_regex
)
from iobs.util import (
    cast_bool,
    try_split
)


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
        """Retrieves the ConfigAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to ConfigAttributes.
        """
        return {
            'enabled': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }


class FIOTemplateConfiguration(TemplateConfiguration):
    pass
