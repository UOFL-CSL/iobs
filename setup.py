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

from setuptools import setup

import iobs


setup(
    name=iobs.__title__,
    version=iobs.__version__,
    description=iobs.__summary__,
    long_description=open('README.md').read(),
    url=iobs.__url__,
    project_urls={
        'UofL CSL': 'http://cecs.louisville.edu/csl/',
        'IOBS source': 'https://github.com/uofl-csl/iobs'
    },

    author=iobs.__author__,
    author_email=iobs.__email__,
    license='GNU GPLv2',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=['iobs', 'iobs.commands'],
    include_package_data=True,

    entry_points={
        'iobs.registered_commands': [
            'execute = iobs.commands.execute:main',
            'validate = iobs.commands.validate:main'
        ],
        'console_scripts': [
            'iobs = iobs.__main__:main'
        ]
    },

    python_requires='>=3.4',
    install_requires=[
        'colorama'
    ]
)
