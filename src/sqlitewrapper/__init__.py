"""
MIT License

Copyright (c) 2023 Jude

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from . import databaseobjects, errors
from . import datatypes as types
from .databaseobjects import DatabaseObject, TableObject

__version__ = '0.1.4'

def Database(
    path: str,
    separate_thread: bool = False,
    await_completion: bool = True
) -> DatabaseObject:
    """ Initiates a database object. """
    return DatabaseObject(path, separate_thread, await_completion)

def Table(
    path: str,
    table: str,
    separate_thread: bool = False,
    await_completion: bool = True
) -> TableObject:
    """ Initiates a table object. """
    database = DatabaseObject(path, separate_thread, await_completion)
    return TableObject(database, table)