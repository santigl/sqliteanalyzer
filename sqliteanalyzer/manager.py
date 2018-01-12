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

"""Helper class to manage queries to an SQLite DB
(file or in-memory)."""

import sqlite3

class SQLite3Manager():
    def __init__(self, db_path=None):
        if db_path is None:
            self._db_connection = sqlite3.connect(":memory:")
            self._db_file = None
        else:
            self._db_file = db_path
            path = 'file:{}?mode=ro'.format(self._db_file)
            self._db_connection = sqlite3.connect(path, uri=True)

            if not self._have_required_compile_flags():
                raise RuntimeError('SQLite lacks a required capability:'
                                   ' DBSTAT_VTAB. Recompile with '
                                   '-DSQLITE_ENABLE_DBSTAT_VTAB')

        self._db_connection.row_factory = sqlite3.Row

    def execute_query(self, *args):
        self._db_connection.cursor().execute(*args)
        self._db_connection.commit()

    def fetch_single_field(self, query):
        cursor = self._create_cursor()
        return tuple(cursor.execute(query).fetchone())[0]

    def fetch_one_row(self, query):
        cursor = self._create_cursor()
        return cursor.execute(query).fetchone()

    def fetch_all_rows(self, query):
        cursor = self._create_cursor()
        return cursor.execute(query).fetchall()

    def iterdump(self):
        return self._db_connection.iterdump()

    def _create_cursor(self):
        return self._db_connection.cursor()

    def _have_required_compile_flags(self):
        has_flag = '''SELECT 1 FROM pragma_compile_options
                      WHERE compile_options="ENABLE_DBSTAT_VTAB"'''

        return bool(self.fetch_single_field(has_flag))
