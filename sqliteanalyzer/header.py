# Copyright 2018 Santiago Gil
# (github.com/santigl)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""Class to parse an SQLite header from a database file."""

from math import log
import sys

class SQLiteHeader:
    """Read an SQLite database file and extract the information
    contained in its header (first ``HEADER_SIZE_BYTES`` = 100 bytes).

    Reference:
        https://sqlite.org/fileformat.html#the_database_header
    """

    HEADER_SIZE_BYTES = 100
    TEXT_ENCODINGS = ['UTF-8', 'UTF-16le', 'UTF-16be']

    def __init__(self, db_path: str):
        """Create an SQLiteHeader instance.

        Arguments:
            db_path: path to an SQLite database file

        Raises:
            OSError, if the database file cannot be opened
        """
        with open(db_path, 'rb') as db_file:
            self._raw_header = bytes(db_file.read(self.HEADER_SIZE_BYTES))
            print(self._raw_header)

        #: str: header string (``'SQLite format 3\x00'``)
        self.header_string = self._raw_header[0:15].decode()

        #: int: page size in bytes
        self.page_size = self._read_int(16, 2)
        if self.page_size == 1:
            self.page_size = 65536


        #: int: SQLite version used in the last read
        self.format_read_version = self._read_int(18)
        #: int: SQLite version used in the last write
        self.format_write_version = self._read_int(19)

        #: int: Unused bytes reserved at the end of a page
        self.reserved_space = self._read_int(20)

        #: int: Maximum embedded payload fraction (must be 64)
        self.max_embedded_payload = self._read_int(21)
        #: int: Minimum embedded payload fraction (must be 32)
        self.min_embedded_payload = self._read_int(22)
        #: int: Leaf payload fraction. Must be 32.
        self.leaf_payload = self._read_int(23)

        #: int: File change counter
        self.change_counter = self._read_int(24, 4)
        #: int: Size of the DB in pages
        self.page_count = self._read_int(28, 4)
        #: int: Page# of the first freelist trunk page
        self.freelist_start = self._read_int(32, 4)
        #: int: Total number of freelist pages
        self.freelist_count = self._read_int(36, 4)
        #: int: Schema cookie
        self.schema_cookie = self._read_int(40, 4)
        #: int: Schema format number
        self.schema_format = self._read_int(44, 4)
        #: int: Default page cache size
        self.page_cache_size = self._read_int(48, 4)

        self.largest_root_page = self._read_int(52, 4)
        """int: Page# of the largest root b-tree page when in
        autovacuum or incremental vacuum modes
        """


        encoding_value = self._read_int(56, 4)
        """str: Text encoding
        ``'UTF-8'``, ``'UTF-16le'``, ``'UTF-16be'`` or ``None`` if
        not valid
        """

        if encoding_value in [1, 2, 3]:
            self.text_encoding = self.TEXT_ENCODINGS[encoding_value-1]
        else:
            self.text_encoding = None

        #: int: User version as set by the ``user_version`` pragma
        self.user_version = self._read_int(60, 4)
        #: bool: Incremental vacuum mode enabled
        self.incremental_vacuum_mode = bool(self._read_int(64, 4))
        #: int: Application ID as set by the ``application_id`` pragma
        self.application_id = self._read_int(68, 4)

        #: int: Bytes reserved by SQLite for expansion. (Must be 0.)
        self.reserved = self._read_int(72, 20)

        #: int: ``version-valid-for`` number
        self.version_valid_for = self._read_int(92, 4)
        #: int: SQLite version number
        self.sqlite_version_number = self._read_int(96, 4)

    def header_seems_valid(self) -> bool:
        """Returns whether the values in fields that have constraints
        do respect them (i.e. the header appears to be valid)."""
        page_size_is_power_of_two = log(self.page_size, 2).is_integer()
        page_size_value_is_valid = self.page_size in range(512, 32769) \
                                   or self.page_size == 65535

        return page_size_is_power_of_two \
               and page_size_value_is_valid \
               and self.format_read_version in [1, 2] \
               and self.format_write_version in [1, 2] \
               and self.max_embedded_payload == 64 \
               and self.min_embedded_payload == 32 \
               and self.leaf_payload == 32 \
               and self.schema_format in [1, 2, 3, 4] \
               and self.text_encoding is not None \
               and self.reserved == 0

    def _read_int(self, start: int, length=1) -> int:
        """Reads length bytes from the start of the header
        and returns the interpreted value as an integer.
        """
        field = self._raw_header[start:start+length]
        return int.from_bytes(field, byteorder='big')

    def __str__(self):
        """Returns a string representation of an SQLite3Header."""

        lines = [
            'Header seems valid? {}'.format(self.header_seems_valid()),
            'Header string: {}'.format(self.header_string),
            'Page size: {}'.format(self.page_size),
            'Format read version: {}'.format(self.format_read_version),
            'Format write version: {}'.format(self.format_write_version),
            'Reserved space: {}'.format(self.reserved_space),
            'Max. embeded payload: {}'.format(self.max_embedded_payload),
            'Min. embeded payload: {}'.format(self.min_embedded_payload),
            'Leaf payload: {}'.format(self.leaf_payload),
            'Change counter: {}'.format(self.change_counter),
            'Page count: {}'.format(self.page_count),
            'Freelist start page: {}'.format(self.freelist_start),
            'Freelist size: {}'.format(self.freelist_count),
            'Schema cookie: {}'.format(self.schema_cookie),
            'Schema format: {}'.format(self.schema_format),
            'Page cache size: {}'.format(self.page_cache_size),
            'Largest b-tree-root page #: {}'.format(self.largest_root_page),
            'Text encoding: {}'.format(self.text_encoding),
            'User version: {}'.format(self.user_version),
            'Incremental vacuum mode: {}'.format(self.incremental_vacuum_mode),
            'Application id.: {}'.format(self.application_id),
            'Version valid for: {}'.format(self.version_valid_for),
            'SQLite version number: {}'.format(self.sqlite_version_number)
            ]

        return '\n'.join(lines)
