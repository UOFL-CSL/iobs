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
    InvalidSettingError
)
from iobs.process import (
    change_nomerges
)
from iobs.util import (
    cast_bool,
    try_split
)


class EnvironmentConfiguration(ConfigSectionBase):
    """Environment Configuration for `environment` section of config."""
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

    def get_environment_permutations(self, device):
        """Creates environment setting permutation generator and applies each.

        Args:
            device: The device.

        Returns:
            Yields tuples of setting permutations.
        """
        if self.enabled:
            for sp in self._get_setting_permutations():
                self.modify_environment(device, sp)
                yield sp
            else:
                yield ()

    def modify_environment(self, device, setting_permutation):
        """Modifies the environment for the given permutated settings.

        Args:
            device: The device.
            setting_permutation: The permutated settings.
        """
        for setting in setting_permutation:
            name, value = setting.split('=')
            if name == 'nomerges':
                change_nomerges(device, value)

    def _get_setting_permutations(self):
        """Retrieves setting permutations.

        Returns:
            A list of setting permutation lists which are mappings of
            `setting_name=setting_value`.
        """
        setting_perm = []

        for setting in self._get_permutate_settings():
            if not self._settings[setting].default_used:
                setting_perm.append([
                    '{}={}'.format(setting, value)
                    for value in getattr(self, setting)
                ])

        return itertools.product(*setting_perm)

    def _get_permutate_settings(self):
        """Retrieves permutatable settings.

        Returns:
            List of settings that are permutatable.
        """
        return [
            'nomerges'
        ]

    def _get_settings(self):
        """Retrieves the ConfigAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to ConfigAttributes.
        """
        return {
            'enabled': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            ),
            'nomerges': ConfigAttribute(
                conversion_fn=lambda x: try_split(x, convert_type=int),
                validation_fn=lambda x: all(i in [0, 1, 2] for i in x),
                default_value=[0]
            )
        }
