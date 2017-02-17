# -*- coding: utf-8 -*-

"""
Config file specifying DBs and table/field relationships
"""

READ_HOST = ''
READ_USER = ''
READ_PASSWORD = ''
READ_DB = ''

WRITE_HOST = ''
WRITE_USER = ''
WRITE_PASSWORD = ''
WRITE_DB = ''

TABLE_MAP = {
    'table1.primary_key': 'table2.foreign_key',
    'table2.primary_key': 'table3.foreign_key',
    'table2.other_key': 'table4.primary_key',
    'table3.other_key': 'table4.primary_key',
}
