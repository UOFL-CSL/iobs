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


class IOBSBaseException(Exception):
    """Base Exception for IOBS"""


class ConfigNotFoundError(IOBSBaseException):
    """Config Not Found Error"""


class InvalidConfigError(IOBSBaseException):
    """Invalid Config Error"""


class InvalidOSError(IOBSBaseException):
    """Invalid OS Error"""


class InvalidPrivilegesError(IOBSBaseException):
    """Invalid Privileges Error"""


class InvalidRegexError(IOBSBaseException):
    """Invalid Regex Error"""


class InvalidSettingError(IOBSBaseException):
    """Invalid Setting Error"""


class JobExecutionError(IOBSBaseException):
    """Job Execution Error"""


class NomergesChangeError(IOBSBaseException):
    """Nomerges Change Error"""


class OutputFileError(IOBSBaseException):
    """Output File Error"""


class OutputFormatError(IOBSBaseException):
    """Output Format Error"""


class OutputParsingError(IOBSBaseException):
    """Output Parsing Error"""


class RetryCountExceededError(IOBSBaseException):
    """Retry Count Exceeded Error"""


class SchedulerChangeError(IOBSBaseException):
    """Scheduler Change Error"""


class UndefinedConstantError(IOBSBaseException):
    """Undefined Constant Error"""


class UndefinedFormatterError(IOBSBaseException):
    """Undefined Formatter Error"""


class UndefinedRegexError(IOBSBaseException):
    """Undefined Regex Error"""


class UndefinedWorkloadTypeError(IOBSBaseException):
    """Undefined Workload Type Error"""


class UninitializedWorkloadConfigurationError(IOBSBaseException):
    """Uninitialized Workload Configuration Error"""
