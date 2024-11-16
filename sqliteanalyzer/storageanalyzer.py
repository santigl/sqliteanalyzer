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

"""Module to extract metrics about the storage use of an
SQLite3 database.
"""

from collections import namedtuple
from math import ceil
from os import stat

from .manager import SQLite3Manager
from .types import Index, IndexListEntry, Page

class StorageMetrics(dict):
    """Storage metrics for a given database object.

    It contains the following keys:

    * ``'nentry'``
    * ``'payload'``
    * ``'ovfl_payload'``
    * ``'mx_payload'``
    * ``'ovfl_cnt'``
    * ``'leaf_pages'``
    * ``'int_pages'``
    * ``'ovfl_pages'``
    * ``'leaf_unused'``
    * ``'int_unused'``
    * ``'ovfl_unused'``
    * ``'gap_cnt'``
    * ``'compressed_size'``
    * ``'depth'``
    * ``'cnt'``
    * ``'total_pages'``
    * ``'total_pages_percent'``
    * ``'storage'``
    * ``'is_compressed'``
    * ``'compressed_overhead'``
    * ``'payload_percent'``
    * ``'total_unused'``
    * ``'total_metadata'``
    * ``'metadata_percent'``
    * ``'average_payload'``
    * ``'average_unused'``
    * ``'average_metadata'``
    * ``'ovfl_percent'``
    * ``'fragmentation'``
    * ``'int_unused_percent'``
    * ``'ovfl_unused_percent'``
    * ``'leaf_unused_percent'``
    * ``'total_unused_percent``

    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

class StorageAnalyzer:
    """Extracts storage-space usage statistics from an SQLite3 database.

        It uses as a starting point the metrics provided by the
        ``DBSTAT`` virtual table.

        Arguments:
            db_path: path to an SQLite3 database file

        Note:
            SQLite3 must have been compiled with the
            ``-DSQLITE_ENABLE_DBSTAT_VTAB`` flag enabled.

        References:
            https://www.sqlite.org/dbstat.html

    """

    def __init__(self, db_path: str):
        self._db_file = db_path
        self._db = SQLite3Manager(self._db_file)

        self._is_compressed = None

        # Creating temporary DBSTAT table:
        self._create_temp_stat_table()

        # Creating in-memory db to store the stats:
        self._stat_db = SQLite3Manager()
        self._stat_db.execute_query(self._spaceused_table_create_query())

        # Gathering the stats for all tables:
        self._compute_stats()

    def item_count(self) -> int:
        """Number of rows defined in table ``SQLITE_MASTER``.

        Returns:
            ``SELECT COUNT(*) from SQLITE_MASTER``
        """
        return self._db.fetch_single_field('''SELECT COUNT(*)
                                              from SQLITE_MASTER''')

    def file_size(self) -> int:
        """Physical size of the database in bytes, as reported by
        :func:`os.stat()`.

        Returns:
            Size of the database [bytes]
        """
        return stat(self._db_file).st_size

    def logical_file_size(self) -> int:
        """Number of bytes that the database should take given the size
        of a page and the number of pages it contains.

        If there is no compression, then this value is equal to
        the physical file size (:func:`file_size`).

        Returns:
            Logical size of the database [bytes]
        """
        return self.page_count() * self.page_size()

    def page_size(self) -> int:
        """Size in bytes of the database pages.

        Returns:
            ``PRAGMA page_size`` [bytes]
        """
        return self._db.fetch_single_field('PRAGMA page_size')

    def page_count(self) -> int:
        """Number of reported pages in the database.

        Returns:
            ``PRAGMA page_count``
        """
        return self._db.fetch_single_field('PRAGMA page_count')

    def calculated_free_pages(self) -> int:
        """Number of free pages.

        Returns:
            :func:`page_count()` - :func:`in_use_pages()`
            - :func:`autovacuum_page_count()`
        """
        return self.page_count()\
               - self.in_use_pages()\
               - self.autovacuum_page_count()

    def calculated_page_count(self) -> int:
        """Number of calculated pages in the database.

        Returns
            The sum of pages in use, pages in the freelist and pages
            in the autovacuum pointer map.

            :func:`page_count()` + :func:`in_use_pages()`
            + :func:`autovacuum_page_count()`
        """
        return self.in_use_pages()\
               + self.freelist_count()\
               + self.autovacuum_page_count()

    def freelist_count(self) -> int:
        """Number of pages in the freelist.

        Those are unused pages in the database.

        Returns:
            ``PRAGMA freelist_count``
        """
        return self._db.fetch_single_field('PRAGMA freelist_count')

    def pages(self) -> [Page]:
        """Returns the definition for all pages in the database.

        It is a dump of the ``DBSTAT`` virtual table.

        Reference:
            https://www.sqlite.org/dbstat.html

        Returns:
            a list of :class:`Page` objects
        """
        query = '''SELECT * FROM temp.stat'''

        return [Page._make(row) for row in self._db.fetch_all_rows(query)]

    def in_use_pages(self) -> int:
        """Number of pages currently in use.

        Returns:
            ``leaf_pages`` + ``internal_pages`` + ``overflow_pages``

        """
        query = '''SELECT sum(leaf_pages+int_pages+ovfl_pages)
                   FROM space_used'''
        return self._stat_db.fetch_single_field(query)

    def in_use_percent(self) -> float:
        """Percentage of pages from the total that are currently in use.

        Returns:
            % of pages of the DB that are currently in use

        """
        return self._percentage(self.in_use_pages(), self.page_count())

    def tables(self) -> [str]:
        """Names of the tables defined in the database.

        Returns:
            tables in the database
        """
        tables = self._db.fetch_all_rows('''SELECT name
                                            FROM sqlite_master
                                            WHERE rootpage>0
                                              AND type == "table"''')
        # Do not include `sqlite_master` because it doesn't hold usage data
        return [t['name'] for t in tables]

    def indices(self) -> [Index]:
        """Returns the indices defined in the database.

        Returns:
            a list of :class:`Index`
        """
        indices = self._db.fetch_all_rows('''SELECT name, tbl_name
                                             FROM sqlite_master
                                             WHERE rootpage>0
                                               AND type == "index"''')

        return [{'name': i['name'], 'tbl_name': i['tbl_name']} \
                for i in indices]

    def index_list(self, table: str) -> [IndexListEntry]:
        """Given a table, returns its entries in ``PRAGMA index_list``.

        Returns:
            A list of :class:`IndexListEntry` namedtuples.

        References:
            https://sqlite.org/pragma.html#pragma_index_list

        """
        query = 'PRAGMA index_list("{}")'.format(table)

        indices = []
        for row in self._db.fetch_all_rows(query):
            index = IndexListEntry(row['seq'], row['name'], bool(row['unique']),
                                   row['origin'], bool(row['partial']))
            indices.append(index)

        return indices

    def ntable(self) -> int:
        """Number of tables in the database."""
        return self._db.fetch_single_field('''SELECT count(*)+1
                                              FROM sqlite_master
                                              WHERE type="table"
                                           ''')

    def nindex(self) -> int:
        """Number of indices in the database."""
        return self._db.fetch_single_field('''SELECT count(*)
                                              FROM sqlite_master
                                              WHERE type="index"
                                           ''')

    def nautoindex(self) -> int:
        """Number of automatically-created indices in the database."""
        return self._db.fetch_single_field('''SELECT count(*)
                                              FROM sqlite_master
                                              WHERE name
                                              LIKE "sqlite_autoindex%"
                                           ''')
    def nmanindex(self)-> int:
        """Number of manually-created indices in the database."""
        return self.nindex() - self.nautoindex()

    def payload_size(self)-> int:
        """Space in bytes used by the user's payload.

        It does not include the space used by the ``sqlite_master``
        table nor any indices.
        """
        return self._stat_db.fetch_single_field('''SELECT sum(payload)
                                                   FROM space_used
                                                   WHERE NOT is_index
                                                     AND name
                                                       NOT LIKE "sqlite_master";
                                                ''')

    def is_compressed(self) -> bool:
        """Returns whether the database file is compressed."""
        if self._is_compressed is None:
            table = self.tables().pop()
            self._is_compressed = self.table_stats(table)['is_compressed']
        return self._is_compressed

    def autovacuum_page_count(self) -> int:
        """The number of pages used by the *auto-vacuum*
        pointer map.
        """
        auto_vacuum = self._db.fetch_single_field('PRAGMA auto_vacuum')
        if auto_vacuum == 0 or self.page_count() == 1:
            return 0

        # The number of entries on each pointer map page.
        #
        # The layout of the database file is one pointer-map
        # page, followed by ptrsPerPage other pages, followed
        # by a pointer-map page, etc.
        #
        # The first pointer-map page is the second page
        # of the file overall.
        page_size = float(self.page_size())
        pointers_per_page = page_size / 5

        # Return the number of pointer map pages
        # in the database.
        return ceil((self.page_count() - 1) / (pointers_per_page + 1))

    def table_space_usage(self) -> dict():
        """Space used by each table in the database.

        Returns:
            A dictionary from table names to page counts.

        """
        # if table is not None:
        #     return self._table_space_usage(table)

        return self._all_tables_usage()

    def table_page_count(self, name: str, exclude_indices=False) -> int:
        """Number of pages that the table is currently using.

        If ``exclude_indices == True``, then it does not count those
        pages taken by indices that might point to that table.

        Args:
            name: name of the table
            exclude_indices: whether to avoid counting pages used
            by indices on the table.
        """
        if exclude_indices:
            return self._item_page_count(name)

        return self._table_space_usage(name)

    def index_page_count(self, name: str) -> int:
        """Number of pages that the index is currently using.

        Args:
            name: name of the index

        Returns:
            number of pages

        """
        return self._item_page_count(name)

    def index_stats(self, name: str) -> StorageMetrics:
        """Returns statistics for the index.

        Args:
            name: name of the index

        Returns:
            a :class:`StorageMetrics` object

        """
        condition = 'name = "{}"'.format(name)
        return self._query_space_used_table(condition)

    def table_stats(self, name: str, exclude_indices=False) -> StorageMetrics:
        """Returns statistics for a table.

        The value of the optional parameter ``exclude_indices``,
        determines whether indices are considered part of the actual
        table or not.

        Args:
            name: name of the table

        Returns:
            a :class:`StorageMetrics` object


        """
        if exclude_indices:
            condition = 'name = "{}"'.format(name)
        else:
            condition = 'tblname = "{}"'.format(name)

        return self._query_space_used_table(condition)

    def global_stats(self, exclude_indices=False) -> StorageMetrics:
        """Storage metrics for all tables and/or indices in the database

        The value of the optional parameter ``exclude_indices``
        determines whether indices are considered.

        Args:
            exclude_indices: bool: if False, space used by indices is
             not considered.

        Returns:
            a StorageMetrics object

        """
        condition = 'NOT is_index' if exclude_indices else '1'
        return self._query_space_used_table(condition)

    def indices_stats(self) -> StorageMetrics:
        """Return metadata about the indices in the database.

        Raises:
            ValueError: If no indices exist

        """
        if not self.nindex():
            raise ValueError('There are no indices in the DB.')

        return self._query_space_used_table('is_index')

    def is_without_rowid(self, table: str) -> bool:
        """Returns whether the given table is a ``WITHOUT ROWID`` table.

        Args:
            table: name of the table

        References:
            https://sqlite.org/withoutrowid.html

        """
        query = 'PRAGMA index_list("{}")'.format(table)
        indices = self._db.fetch_all_rows(query)

        for index in indices:
            if index['origin'].upper() == 'PK':
                query = '''SELECT count(*)
                           FROM sqlite_master
                           WHERE name="{}"'''.format(table)

                pk_is_table = self._db.fetch_single_field(query)
                if not pk_is_table:
                    return True

        return False

    def stat_db_dump(self) -> [str]:
        """Returns a dump of the DB containing the stats.

        Returns:
            list of lines containing an SQL dump of the stat database.

        """
        return list(self._stat_db.iterdump())

#### HELPERS ####

    def _query_space_used_table(self, where: str) -> StorageMetrics:
        # total_pages: Database pages consumed.
        # total_pages_percent: Pages consumed as a percentage of the file.
        # storage: Bytes consumed.
        # payload_percent: Payload bytes used as a percentage of $storage.
        # total_unused: Unused bytes on pages.
        # avg_payload: Average payload per btree entry.
        # avg_fanout: Average fanout for internal pages.
        # avg_unused: Average unused bytes per btree entry.
        # avg_meta: Average metadata overhead per entry.
        # ovfl_cnt_percent: Percentage of btree entries that use overflow pages.
        query = '''SELECT
                   sum(
                    CASE WHEN (is_without_rowid OR is_index) THEN nentry
                         ELSE leaf_entries
                    END
                   ) AS nentry,
                   sum(payload) AS payload,
                   sum(ovfl_payload) AS ovfl_payload,
                   max(mx_payload) AS mx_payload,
                   sum(ovfl_cnt) as ovfl_cnt,
                   sum(leaf_pages) AS leaf_pages,
                   sum(int_pages) AS int_pages,
                   sum(ovfl_pages) AS ovfl_pages,
                   sum(leaf_unused) AS leaf_unused,
                   sum(int_unused) AS int_unused,
                   sum(ovfl_unused) AS ovfl_unused,
                   sum(gap_cnt) AS gap_cnt,
                   sum(compressed_size) AS compressed_size,
                   max(depth) AS depth,
                   count(*) AS cnt
                   FROM space_used
                   WHERE {}
                '''.format(where)

        stats = self._stat_db.fetch_one_row(query)
        s = self._extract_storage_metrics(stats)

        # Adding calculated values:
        s['total_pages'] = s['leaf_pages']\
                           + s['int_pages']\
                           + s['ovfl_pages']

        s['total_pages_percent'] = self._percentage(s['total_pages'],
                                                    self.page_count())

        s['storage'] = s['total_pages'] * self.page_size()

        s['is_compressed'] = (s['storage'] > s['compressed_size'])

        s['compressed_overhead'] = 14 if s['is_compressed'] \
                                   else 0

        s['payload_percent'] = self._percentage(s['payload'],
                                                s['storage'])

        s['total_unused'] = s['ovfl_unused']\
                          + s['int_unused'] \
                          + s['leaf_unused']

        s['total_metadata'] = s['storage'] - s['payload']\
                            - s['total_unused']\
                            + 4 * (s['ovfl_pages'] - s['ovfl_cnt'])

        s['metadata_percent'] = self._percentage(s['total_metadata'],
                                                 s['storage'])

        if s['nentry'] == 0:
            s['average_payload'] = 0
            s['average_unused'] = s['average_metadata'] = 0
        else:
            s['average_payload'] = s['payload'] / s['nentry']
            s['average_unused'] = s['total_unused'] / s['nentry']
            s['average_metadata'] = s['total_metadata'] / s['nentry']


        s['ovfl_percent'] = self._percentage(s['ovfl_cnt'], s['nentry'])

        s['fragmentation'] = self._percentage(s['gap_cnt'],
                                              s['total_pages'] - 1)

        s['int_unused_percent'] = self._percentage(s['int_unused'],
                                                   s['int_pages']\
                                                   * self.page_size())

        s['ovfl_unused_percent'] = self._percentage(s['ovfl_unused'],
                                                    s['ovfl_pages']\
                                                    * self.page_size())

        s['leaf_unused_percent'] = self._percentage(s['leaf_unused'],
                                                    s['leaf_pages']\
                                                    * self.page_size())

        s['total_unused_percent'] = self._percentage(s['total_unused'],
                                                     s['storage'])

        return s


    def _item_page_count(self, name: str) -> int:
        query = '''SELECT (int_pages + leaf_pages + ovfl_pages)
                   FROM space_used
                   WHERE name = "{}"
                 '''.format(name)
        return self._stat_db.fetch_single_field(query)

    def _table_space_usage(self, tbl_name: str) -> int:
        query = '''SELECT
                    sum(int_pages + leaf_pages + ovfl_pages)
                      AS pages
                   FROM space_used
                   WHERE tblname = "{}"
                   GROUP BY tblname
                '''.format(tbl_name)

        return self._stat_db.fetch_single_field(query)

    def _all_tables_usage(self) -> dict():
        query = '''SELECT tblname as name,
                          sum(int_pages + leaf_pages + ovfl_pages)
                            AS pages
                   FROM space_used
                   GROUP BY tblname'''

        return {row['name']: row['pages'] \
                for row in self._stat_db.fetch_all_rows(query)}

    def _compute_stats(self):
        tables = [{'name': t, 'tbl_name': t} for t in self.tables()]
        indices = self.indices()

        for entry in tables + indices:
            stats = self._extract_sqlite_stats(entry['name'])

            is_index = (entry['name'] != entry['tbl_name'])

            values = (entry['name'],
                      entry['tbl_name'],
                      is_index,
                      stats['is_without_rowid'],
                      stats['nentry'],
                      stats['leaf_entries'],
                      stats['depth'],
                      stats['payload'],
                      stats['ovfl_payload'],
                      stats['ovfl_cnt'],
                      stats['mx_payload'],
                      stats['int_pages'],
                      stats['leaf_pages'],
                      stats['ovfl_pages'],
                      stats['int_unused'],
                      stats['leaf_unused'],
                      stats['ovfl_unused'],
                      stats['gap_count'],
                      stats['compressed_size'])

            placeholders = ','.join('?' * len(values))
            insert_query = '''INSERT INTO space_used
                              VALUES ({})'''.format(placeholders)

            self._stat_db.execute_query(insert_query, values)



### HELPERS ###
    def _count_gaps(self, table_name: str):
    # Column 'gap_cnt' is set to the number of non-contiguous entries in the
    # list of pages visited if the b-tree structure is traversed in a top-
    # down fashion (each node visited before its child-tree is passed). Any
    # overflow chains present are traversed from start to finish before any
    # child-tree is.
        pages = self._db.fetch_all_rows('''SELECT pageno, pagetype
                                           FROM temp.dbstat
                                           WHERE name="{}"
                                           ORDER BY pageno;
                                        '''.format(table_name))
        gap_count = 0
        previous_page = 0
        for page in pages:
            if previous_page > 0 and (page['pagetype'] == 'leaf') \
               and (page['pageno'] != previous_page+1):
                gap_count += 1

            previous_page = page['pageno']

        return gap_count

    def _extract_sqlite_stats(self, table_name: str) -> dict:
        query = '''SELECT
                sum(ncell) AS nentry,
                sum((pagetype == 'leaf') * ncell) AS leaf_entries,
                sum(payload) AS payload,
                sum((pagetype == 'overflow') * payload) AS ovfl_payload,
                sum(path LIKE '%+000000') AS ovfl_cnt,
                max(mx_payload) AS mx_payload,
                sum(pagetype == 'internal') AS int_pages,
                sum(pagetype == 'leaf') AS leaf_pages,
                sum(pagetype == 'overflow') AS ovfl_pages,
                sum((pagetype == 'internal') * unused) AS int_unused,
                sum((pagetype == 'leaf') * unused) AS leaf_unused,
                sum((pagetype == 'overflow') * unused) AS ovfl_unused,
                sum(pgsize) AS compressed_size,
                max((length(CASE WHEN path LIKE '%+%' THEN ''
                                 ELSE path END)+3)/4) AS depth
                FROM temp.dbstat
                WHERE name = '{}';'''.format(table_name)


        stats = self._row_to_dict(self._db.fetch_one_row(query))
        stats['is_without_rowid'] = self.is_without_rowid(table_name)
        stats['gap_count'] = self._count_gaps(table_name)

        return stats

    @staticmethod
    def _row_to_dict(row) -> dict:
        """Convert an sqlite.row to a regular dictionary."""
        res = {}
        for column in row.keys():
            res[column] = row[column]

        return res

    @staticmethod
    def _extract_storage_metrics(row) -> StorageMetrics:
        """Convert an sqlite.row to a StorageMetrics object."""
        res = StorageMetrics()
        for column in row.keys():
            res[column] = row[column]

        return res

    @staticmethod
    def _percentage(value: float, total: float) -> float:
        if total == 0:
            return 0
        return 100 * value / total

    def _create_stat_virtual_table(self):
        self._db.execute_query('''CREATE VIRTUAL TABLE temp.stat
                                  USING dbstat''')

    def _create_temp_stat_table(self):
        self._create_stat_virtual_table()

        self._db.execute_query('''CREATE TEMP TABLE dbstat
                                 AS SELECT * FROM temp.stat
                                 ORDER BY name, path''')

    @staticmethod
    def _stat_table_create_query():
        return '''CREATE TABLE stats("
                  name       STRING,  /* Name of table or index */
                  path       INTEGER, /* Path to page from root */
                  pageno     INTEGER, /* Page number */
                  pagetype   STRING,  /* 'internal', 'leaf' or 'overflow' */
                  ncell      INTEGER, /* Cells on page (0 for overflow) */
                  payload    INTEGER, /* Bytes of payload on this page */
                  unused     INTEGER, /* Bytes of unused space on this page */
                  mx_payload INTEGER, /* Largest payload size of all cells */
                  pgoffset   INTEGER, /* Offset of page in file */
                  pgsize     INTEGER  /* Size of the page */
            ");'''

    @staticmethod
    def _spaceused_table_create_query():
        return '''CREATE TABLE space_used(
                  name clob,        -- Name of a table or index in the database file
                  tblname clob,     -- Name of associated table
                  is_index boolean, -- TRUE if it is an index, false for a table
                  is_without_rowid boolean, -- TRUE if WITHOUT ROWID table
                  nentry int,       -- Number of entries in the BTree
                  leaf_entries int, -- Number of leaf entries
                  depth int,        -- Depth of the b-tree
                  payload int,      -- Total amount of data stored in this table or index
                  ovfl_payload int, -- Total amount of data stored on overflow pages
                  ovfl_cnt int,     -- Number of entries that use overflow
                  mx_payload int,   -- Maximum payload size
                  int_pages int,    -- Number of interior pages used
                  leaf_pages int,   -- Number of leaf pages used
                  ovfl_pages int,   -- Number of overflow pages used
                  int_unused int,   -- Number of unused bytes on interior pages
                  leaf_unused int,  -- Number of unused bytes on primary pages
                  ovfl_unused int,  -- Number of unused bytes on overflow pages
                  gap_cnt int,      -- Number of gaps in the page layout
                  compressed_size int  -- Total bytes stored on disk
                )'''
