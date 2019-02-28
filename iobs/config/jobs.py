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
import json

from iobs.output import printf, PrintType
from iobs.errors import (
    JobExecutionError,
    OutputParsingError
)
from iobs.process import (
    change_scheduler,
    clear_caches,
    run_command
)


class Job(ABC):
    """A single unit of work to be executed.

    Args:
        file: The input file.
        device: The device.
        scheduler: The scheduler.
    """
    def __init__(self, file, device, scheduler):
        self.file = file
        self.device = device
        self.scheduler = scheduler

    def process(self):
        """Processes the job.

        Returns:
            The output of the job.
        """
        change_scheduler(self.device, self.scheduler)
        clear_caches(self.device)
        return self.execute()

    @abstractmethod
    def execute(self):
        """Executes the job."""


class FIOJob(Job):
    """An FIO Job."""
    def get_command(self):
        """Retrieves the command to execute.

        Returns:
            The command string.
        """
        return 'fio {} --output-format=json'.format(self.file)

    def collect_output(self, output):
        """Collects the output metrics from the job execution.

        Args:
            output: The raw output.

        Returns:
            A dictionary mapping metric names to values.

        Raises:
            OutputParsingError: If unable to parse raw output.
        """
        try:
            data = json.loads(output, encoding='utf-8')
            job_data = data['jobs'][0]

            metrics = {
                **self._parse_job_other(job_data),
                **self._parse_job_rw(job_data['read'], 'read'),
                **self._parse_job_rw(job_data['write'], 'write')
            }

            return metrics
        except (json.JSONDecodeError, KeyError, IndexError) as err:
            raise OutputParsingError(
                'Unable to parse output\n{}'.format(err)
            )

    def _parse_job_rw(self, data, rw):
        """Parses the job data from the raw output.

        Args:
            data: The data to parse.
            rw: Either 'read' or 'write'.

        Returns:
            A dictionary mapping the metric names to their values.
        """
        metrics = {
            'total-ios-{}'.format(rw): data['total_ios'],  # IO
            'io-kbytes-{}'.format(rw): data['io_kbytes'],  # KB
            'bw-{}'.format(rw): data['bw'],  # MB/s
            'iops-{}'.format(rw): data['iops'],  # IO/s
            'lat-min-{}'.format(rw): data['lat_ns']['min'],  # ns
            'lat-max-{}'.format(rw): data['lat_ns']['max'],  # ns
            'lat-mean-{}'.format(rw): data['lat_ns']['mean'],  # ns
            'lat-stddev-{}'.format(rw): data['lat_ns']['stddev'],  # ns
            'slat-min-{}'.format(rw): data['slat_ns']['min'],  # ns
            'slat-max-{}'.format(rw): data['slat_ns']['max'],  # ns
            'slat-mean-{}'.format(rw): data['slat_ns']['mean'],  # ns
            'slat-stddev-{}'.format(rw): data['slat_ns']['stddev'],  # ns
            'clat-min-{}'.format(rw): data['clat_ns']['min'],  # ns
            'clat-max-{}'.format(rw): data['clat_ns']['max'],  # ns
            'clat-mean-{}'.format(rw): data['clat_ns']['mean'],  # ns
            'clat-stddev-{}'.format(rw): data['clat_ns']['stddev']  # ns
        }

        if 'percentile' in data['lat_ns']:
            for p, v in data['lat_ns']['percentile'].items():
                metrics['lat-percentile-{}-{}'.format(p, rw)] = v  # ns

        if 'percentile' in data['clat_ns']:
            for p, v in data['clat_ns']['percentile'].items():
                metrics['clat-percentile-{}-{}'.format(p, rw)] = v  # ns

        return metrics

    def _parse_job_other(self, data):
        """Parses the other data from the raw output.

        Args:
            data: The data to parse.

        Returns:
            A dictionary mapping the metric names to their values.
        """
        return {
            'job-runtime': data['job_runtime']  # ms
        }

    def execute(self):
        """Executes the job.

        Returns:
            The collected output metrics.

        Raises JobExecutionError: If job failed to run.
        """
        command = self.get_command()
        out, _ = run_command(command)

        if out is None:
            raise JobExecutionError(
                'Unable to run command {} for device {}'
                .format(command, self.device)
            )

        printf('Job output:\n{}'.format(out), print_type=PrintType.DEBUG_LOG)

        return self.collect_output(out)
