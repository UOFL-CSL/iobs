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


from iobs.config.base import (
    ConfigAttribute,
    ConfigSectionBase
)
from iobs.errors import (
    InvalidSettingError
)
from iobs.process import (
    is_block_devices,
    validate_schedulers
)
from iobs.settings import (
    is_valid_workload_type
)
from iobs.util import (
    cast_bool,
    try_split
)


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
        """Retrieves the ConfigAttributes for the configuration object.

        Returns:
            A dictionary mapping of setting names to ConfigAttributes.
        """
        return {
            'workload_type': ConfigAttribute(
                validation_fn=is_valid_workload_type
            ),
            'devices': ConfigAttribute(
                conversion_fn=try_split,
                validation_fn=is_block_devices
            ),
            'schedulers': ConfigAttribute(
                conversion_fn=try_split,
                validation_fn=lambda x: validate_schedulers(x, self.devices),
                dependent_attributes=['devices']
            ),
            'repetitions': ConfigAttribute(
                conversion_fn=int,
                validation_fn=lambda x: x >= 1,
                default_value=1
            ),
            'enable_blktrace': ConfigAttribute(
                conversion_fn=cast_bool,
                default_value=False
            )
        }
