from collections import namedtuple

Page = namedtuple('Page', ('name', 'path', 'pageno', 'pagetype', 'ncell',
                           'payload', 'unused', 'mx_payload', 'pgoffset',
                           'pgsize'))
Page.name.__doc__ = 'Name of table or index'
Page.path.__doc__ = 'Path from the root to the page'
Page.pageno.__doc__ = 'Page number'
Page.pagetype.__doc__ = "``'internal'``, ``'leaf'`` or ``'overflow'``"
Page.ncell.__doc__ = 'Cells on page (0 for overflow)'
Page.payload.__doc__ = 'Bytes of payload on the page'
Page.unused.__doc__ = 'Bytes of unused space on the page'
Page.mx_payload.__doc__ = 'Largest payload size of all cells on the page'
Page.pgoffset.__doc__ = 'Offset of the page in the file'
Page.pgsize.__doc__ = 'Size of the page'



Index = namedtuple('Index', ('name', 'table'))

Index.name.__doc__ = 'str: Name of the index'
Index.table.__doc__ = 'str: Table to which the index points'


IndexListEntry = namedtuple('IndexListEntry', ('seq', 'name', 'unique',
                                               'origin', 'partial'))

IndexListEntry.seq.__doc__ = 'int: internal sequence number of the index'
IndexListEntry.name.__doc__ = 'str: name of the index'
IndexListEntry.unique.__doc__ = 'bool: whether index is ``UNIQUE``'

IndexListEntry.origin.__doc__ = 'str: How was the index created. '\
                                '``c`` if it was created by a ' \
                                '``CREATE_INDEX`` statement, ' \
                                '``u`` if created by a ``UNIQUE`` '\
                                'constraint, or ``pk`` if created by ' \
                                'a ``PRIMARY_KEY`` constraint'

IndexListEntry.partial.__doc__ = 'bool: whether the index covers ' \
                                ' only a subset of rows of a table'
