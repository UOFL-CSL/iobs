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


from functools import wraps


def cast_bool(obj):
    """Attempts to cast an object as a bool using the following rules:
        1. If int: True if 1, else False.
        2. If str: True if lower-cased is 't' or 'true', else False.
        3. bool(obj)

    Args:
        obj:  The object to cast.

    Returns:
        Boolean representation of object.
    """
    if isinstance(obj, int):
        return obj == 1

    if isinstance(obj, str):
        return obj.lower() in ('t', 'true', '1')

    return bool(obj)


def ignore_exception(exception, default_val):
    """A decorator function.

    A decorator function that ignores the exception raised, and instead returns
    a default value.

    Args:
        exception: The exception to catch.
        default_val: The decorated function.

    Returns:
        The decorator function.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception:
                return default_val
        return wrapper
    return decorator


def try_split(s, delimiter=','):
    """Tries to split a string by the given delimiter(s).

    Args:
        s: The string to split.
        delimiter: Either a single string, or a tuple of strings
            (i.e. (',', ';').

    Returns:
        The string split into a list.
    """
    if isinstance(delimiter, tuple):
        for d in delimiter:
            if d in s:
                return [i.strip() for i in s.split(d)]
    elif delimiter in s:
        return s.split(delimiter)

    return [s]
