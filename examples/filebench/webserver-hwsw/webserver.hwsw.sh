#!/usr/bin/env bash
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

iobs execute webserver.hwsw.iobs -l webserver.hwsw.log -d -x
python webserver-hwsw-graph.py webserver.hwsw.csv -d /dev/nvme0n1=3DXP /dev/nvme1n1=NVME /dev/sdb=SSD /dev/sdd=HDD
