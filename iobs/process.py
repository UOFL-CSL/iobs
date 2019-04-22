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


from collections import namedtuple
import os
import shlex
import signal
import stat
import subprocess

from iobs.errors import (
    DeviceSettingChangeError,
    SchedulerChangeError
)
from iobs.output import printf, PrintType
from iobs.settings import (
    match_regex
)


CommandProcess = namedtuple('CommandProcess', ('command', 'process'))


class ProcessManager:
    PROCESSES = []

    @staticmethod
    def add_process(command, process):
        """Tracks a running process.

        Args:
            command: The command being run in the process.
            process: The process.
        """
        ProcessManager.PROCESSES.append(CommandProcess(command, process))

    @staticmethod
    def clear_finished_processes():
        """Returns finished processes.

        Returns:
            A list of finished CommandProcesses."""
        finished_processes = []
        process_index = 0

        while process_index < len(ProcessManager.PROCESSES):
            process = ProcessManager.PROCESSES[process_index]
            if process[1].poll() in (None, 0):
                ProcessManager.PROCESSES[process_index], ProcessManager.PROCESSES[-1] = \
                    ProcessManager.PROCESSES[-1], ProcessManager.PROCESSES[process_index]
                finished_processes.append(ProcessManager.PROCESSES[-1])
                ProcessManager.PROCESSES.pop()
            else:
                process_index += 1

        return finished_processes

    @staticmethod
    def clear_processes():
        """Clears all tracked processes."""
        ProcessManager.PROCESSES.clear()

    @staticmethod
    def clear_process(command):
        """Clears a specific process by it's command."""
        for i, p in enumerate(ProcessManager.PROCESSES):
            if p.command == command:
                ProcessManager.PROCESSES[i], ProcessManager.PROCESSES[-1] = \
                    ProcessManager.PROCESSES[-1], ProcessManager.PROCESSES[i]
                ProcessManager.PROCESSES.pop()
                break

    @staticmethod
    def failed_processes():
        """Returns the processes which are failed.

        Returns:
            A list of failed CommandProcesses.
        """
        # Returns code other than 0 indicates error
        return [p for p in ProcessManager.PROCESSES if p[1].poll() not in (None, 0)]

    @staticmethod
    def finished_processes():
        """Returns the processes which are finished.

        Returns:
            A list of finished CommandProcesses.
        """
        # Return code 0 indicates success
        return [p for p in ProcessManager.PROCESSES if p[1].poll() in (None, 0)]

    @staticmethod
    def has_current_processes():
        """Returns whether there are any tracked processes currently running.

        Returns:
            Number of running processes.
        """
        return len(ProcessManager.PROCESSES) != 0

    @staticmethod
    def kill_processes():
        """Kills the processes."""
        printf('Killing running processes...', print_type=PrintType.DEBUG_LOG)

        for command_name, process in ProcessManager.PROCESSES:
            try:
                printf('Killing process %s [%s]' % (command_name, process.id),
                       print_type=PrintType.DEBUG_LOG)
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except Exception as err:
                printf('Failed to kill process: %s [%s]\n%s' % (command_name, process.pid, err),
                       print_type=PrintType.ERROR_LOG)

    @staticmethod
    def print_processes():
        """Prints the output of each process."""
        printf('Outputting running process information...',
               print_type=PrintType.DEBUG_LOG)

        for command_name, process in ProcessManager.PROCESSES:
            out, err = process.communicate()
            if out:
                printf('command {} output\n{}'
                       .format(command_name, out.decode('utf-8')),
                       print_type=PrintType.DEBUG_LOG)

            if err:
                printf('command {} error\n{}'
                       .format(command_name, err.decode('utf-8')),
                       print_type=PrintType.DEBUG_LOG)


def change_nomerges(device, nomerges):
    """Changes the nomerges setting for the given device.

    Args:
        device: The device.
        nomerges: The nomerges setting.

    Returns:
        True if successful, else False.
    """
    printf('Changing nomerges for device {} to {}'.format(device, nomerges),
           print_type=PrintType.DEBUG_LOG)

    command = 'bash -c "echo {} > /sys/block/{}/queue/nomerges"' \
              .format(nomerges, match_regex(device, 'device_name'))

    _, rc = run_command(command)

    if rc != 0:
        raise DeviceSettingChangeError(
            'Unable to change nomerges to {} for device {}'
            .format(nomerges, device)
        )


def change_scheduler(device, scheduler):
    """Changes the I/O scheduler for the given device.

    Args:
        device: The device.
        scheduler: The I/O scheduler.

    Returns:
        True if successful, else False.
    """
    printf('Changing scheduler for device {} to {}'.format(device, scheduler),
           print_type=PrintType.DEBUG_LOG)

    command = 'bash -c "echo {} > /sys/block/{}/queue/scheduler"' \
              .format(scheduler, match_regex(device, 'device_name'))

    _, rc = run_command(command)

    if rc != 0:
        raise SchedulerChangeError(
            'Unable to change scheduler to {} for device {}'
            .format(scheduler, device)
        )


def check_command(command):
    """Returns whether the given command exists on the system.

    Args:
        command: The command.

    Returns:
        True if exists, else False.
    """
    printf('Checking if command {} exists'.format(command),
           print_type=PrintType.DEBUG_LOG)

    if run_system_command('command -v {}'.format(command)) == 0:
        return True

    printf('Command {} does not exist'.format(command),
           print_type=PrintType.ERROR_LOG)

    return False


def check_commands(commands):
    """Checks whether the required utilities are available on the system.

    Returns:
        True if all commands are valid, else False.
    """
    return all(check_command(c) for c in commands)


def clear_caches(device):
    """Clears various data caches. Should be run before each benchmark.

    Args:
        device: The device.
    """
    printf('Clearing caches for device {}'.format(device),
           print_type=PrintType.DEBUG_LOG)

    # Writes any data buffered in memory out to disk
    run_system_command('sync')

    # Drops clean caches
    run_system_command('echo 3 > /proc/sys/vm/drop_caches')

    # Calls block device ioctls to flush buffers
    run_system_command('blockdev --flushbufs {}'.format(device))

    # Flushes the on-drive write cache buffer
    run_system_command('hdparm -F {}'.format(device))


def cleanup_files(files):
    """Removes the specified file, or files if multiple are given.

    Args:
        files: The files to remove.
    """
    printf('Cleaning up files', print_type=PrintType.DEBUG_LOG)

    if isinstance(files, str):
        files = files.split(' ')

    for file in files:
        printf('Removing files %s' % file, print_type=PrintType.DEBUG_LOG)
        if run_system_command('rm -f {}'.format(file)) != 0:
            printf('Unable to clean up files: {}'.format(file),
                   print_type=PrintType.ERROR_LOG)


def get_device_major_minor(device):
    """Returns a string of the major, minor of a given device.

    Args:
        device: The device.

    Returns:
        A string of major,minor.
    """
    printf('Retrieving major,minor for device {}'.format(device),
           print_type=PrintType.DEBUG_LOG)

    out, _ = run_command('stat -c \'%%t,%%T\' {}'.format(device))

    if not out:
        printf('Unable to retrieve major,minor information for device {}'
               .format(device),
               print_Type=PrintType.ERROR_LOG)
        return None

    out = out.strip()
    printf('major,minor for device {} is {}'.format(device, out),
           print_Type=PrintType.DEBUG_LOG)

    return out


def get_device_nomerges(device):
    """Returns the current nomerges for the device.

    Args:
        device: The device.

    Returns:
        The current nomerges setting.
    """
    printf('Retrieving nomerges for device {}'.format(device),
           print_type=PrintType.DEBUG_LOG)

    device_name = match_regex(device, 'device_name')

    out, rc = run_command('cat /sys/block/{}/queue/nomerges'.format(device_name))

    if rc != 0:
        printf('Unable to find nomerges for device',
               print_type=PrintType.ERROR_LOG)
        return []

    return int(out)


def get_device_scheduler(device):
    """Returns the current scheduler for the device.

    Args:
        device: The device.

    Returns:
        The current scheduler.
    """
    printf('Retrieving schedulers for device {}'.format(device),
           print_type=PrintType.DEBUG_LOG)

    device_name = match_regex(device, 'device_name')

    out, rc = run_command('cat /sys/block/{}/queue/scheduler'.format(device_name))

    if rc != 0:
        printf('Unable to find schedulers for device',
               print_type=PrintType.ERROR_LOG)
        return []

    l, r = out.index('['), out.index(']')
    ret = out[l+1:r]

    printf('Found the current scheduler for device {}: '
           '{}'.format(device, ret),
           print_type=PrintType.DEBUG_LOG)

    return ret


def get_schedulers_for_device(device):
    """Returns a list of available schedulers for a given device.

    Args:
        device: The device.

    Returns:
        A list of schedulers.
    """
    printf('Retrieving schedulers for device {}'.format(device),
           print_type=PrintType.DEBUG_LOG)

    device_name = match_regex(device, 'device_name')

    out, rc = run_command('cat /sys/block/{}/queue/scheduler'.format(device_name))

    if rc != 0:
        printf('Unable to find schedulers for device',
               print_type=PrintType.ERROR_LOG)
        return []

    ret = out.strip().replace('[', '').replace(']', '')

    printf('Found the following schedulers for device {}: '
           '{}'.format(device, ret),
           print_type=PrintType.DEBUG_LOG)

    return ret.split()


def is_block_device(device):
    """Returns whether the given device is a valid block device.

    Args:
        device: The device.

    Returns:
        True if is a valid block device, else False.
    """
    printf('Checking if device {} is a valid block device'.format(device),
           print_type=PrintType.DEBUG_LOG)

    try:
        if stat.S_ISBLK(os.stat(device).st_mode):
            printf('Device {} is a valid block device'.format(device),
                   print_type=PrintType.DEBUG_LOG)
            return True

        printf('Device {} is not a valid block device'.format(device),
               print_type=PrintType.ERROR_LOG)
        return False
    except (FileNotFoundError, TypeError):
        printf('Device {} is not a valid block device'.format(device),
               print_type=PrintType.ERROR_LOG)
        return False


def is_block_devices(devices):
    """Returns whether the given devices are valid block devices.

    Args:
        devices: The devices.

    Returns:
        True if all are valid block device, else False.
    """
    return all(is_block_device(device) for device in devices)


def is_rotational_device(device):
    """Returns whether the given device is a rotational device.

    Args:
        device: The device.

    Returns:
        True if is a rotational device, else False.
    """
    printf('Checking whether device {} is a rotational device'.format(device),
           print_type=PrintType.DEBUG_LOG)

    device_name = match_regex(device, 'device_name')

    if not device_name:
        return False

    out, rc = run_command(
        'cat /sys/block/{}/queue/rotational'.format(device_name)
    )

    if rc != 0:
        return False

    if int(out) == 1:
        printf('Device {} is a rotational device'.format(device),
               print_type=PrintType.DEBUG_LOG)
    else:
        printf('Device {} is not a rotational device'.format(device),
               print_type=PrintType.DEBUG_LOG)

    return int(out) == 1


def run_command(command, ignore_output=False):
    """Runs a command via subprocess communication.

    Args:
        command: The command.
        ignore_output: (OPTIONAL) Whether to ignore the output. Defaults to
            False.

    Returns:
        A tuple containing (the output, the return code).
    """
    printf('Running command {}'.format(command), print_type=PrintType.DEBUG_LOG)

    try:
        args = shlex.split(command)
        p = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            preexec_fn=os.setsid)

        ProcessManager.add_process(command, p)

        if ignore_output:
            return None, _wait_for_process(command, p)

        return _communicate_to_process(command, p)
    except (ValueError, subprocess.CalledProcessError, FileNotFoundError) as err:
        printf('Command {} erred:\n{}'.format(command, err),
               print_type=PrintType.ERROR_LOG)
        return None, None
    finally:
        ProcessManager.clear_process(command)


def _wait_for_process(command, p):
    """Waits for a process to complete.

    Args:
        command: The command.
        p: The process.

    Returns:
        The return code.
    """
    rc = p.wait()

    if rc != 0:
        printf('Command {} [{}] erred with return code {}'
               .format(command, p.pid, rc),
               print_type=PrintType.ERROR_LOG)
    return rc


def _communicate_to_process(command, p):
    """Communicates to a process.

    Args:
        command: The command.
        p: The process.

    Returns:
        A tuple of the output and return code.
    """
    out, err = p.communicate()
    rc = p.returncode

    if err:
        printf('Command {} [{}] erred with return code {}:\n{}'
               .format(command, p.pid, rc, err.decode('utf-8')),
               print_type=PrintType.ERROR_LOG)

    return out.decode('utf-8'), rc


def run_command_nowait(command):
    """Runs a command via subprocess communication, and does not communicate
       with it for completion. It instead returns the process so the caller
       can handle it.

    Args:
        command: The command.

    Returns:
        The Process, or None if erred.
    """
    printf('Running command {}'.format(command), print_type=PrintType.DEBUG_LOG)

    try:
        args = shlex.split(command)
        p = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            preexec_fn=os.setsid)

        ProcessManager.add_process(command, p)
        return p

    except (ValueError, subprocess.CalledProcessError, FileNotFoundError) as err:
        printf('Command {} erred:\n{}'.format(command, err),
               print_type=PrintType.ERROR_LOG)
        ProcessManager.clear_process(command)
        return None


def run_system_command(command, silence=True):
    """Runs a system command.

    Args:
        command: The command.
        silence: (OPTIONAL) Whether to silence the console output. Defaults to
            True.

    Returns:
        The return code.
    """
    if silence:
        command = '{} >/dev/null 2>&1'.format(command)

    printf('Running command {}'.format(command),
           print_type=PrintType.DEBUG_LOG)

    try:
        return os.system(command)
    except Exception as err:
        printf('Error occurred running command {}\n{}'.format(command, err),
               print_type=PrintType.ERROR_LOG)
        return -1


def terminate_process(process):
    """Terminates a process.

    Args:
        process: The process.

    Returns:
        A tuple of the output and return code.
    """
    process.terminate()
    out, err = process.communicate()
    rc = process.returncode

    if err:
        printf('Process [{}] erred with return code {}:\n{}'
               .format(process.pid, rc, err.decode('utf-8')),
               print_type=PrintType.ERROR_LOG)

    return out.decode('utf-8'), rc


def validate_schedulers(schedulers, devices):
    """Validates all schedulers are available on the devices.

    Args:
        schedulers: The schedulers.
        devices: The devices.

    Returns:
        True if all are valid, else False.
    """
    return all(
        validate_schedulers_for_device(schedulers, device)
        for device in devices
    )


def validate_schedulers_for_device(schedulers, device):
    """Validates the schedulers are available on the device.

    Args:
        schedulers: The schedulers.
        device: The device.

    Returns:
        True if all are valid, else False.
    """
    valid_schedulers = set(get_schedulers_for_device(device))
    return all(sched in valid_schedulers for sched in schedulers)
