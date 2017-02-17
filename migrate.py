#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Simple script to migrate data from one database to another given
a starting point
"""

import sys
import logging
logging.basicConfig(level=logging.DEBUG)

import MySQLdb
import config

class Migrator:
    """ Class to help migrate data from one set of tabls to another
    """
    def __init__(self, table_map, dry_run = True):
        self.table_map = table_map
        self.read_db = MySQLdb.connect(
            host=config.READ_HOST,
            user=config.READ_USER,
            passwd=config.READ_PASSWORD,
            db=config.READ_DB)
        self.write_db = MySQLdb.connect(
            host=config.WRITE_HOST,
            user=config.WRITE_USER,
            passwd=config.WRITE_PASSWORD,
            db=config.WRITE_DB)
        self.parse_map()
        self.dry_run = dry_run
        self.read_cursor = self.read_db.cursor()
        self.write_cursor = self.write_db.cursor()

    def __del__(self):
        if self.read_db:
            try:
                self.read_db.close()
            except Exception, e:
                pass
        if self.write_db:
            try:
                self.write_db.commit()
                self.write_db.close()
            except Exception, e:
                pass

    def parse_map(self):
        """
        Go from table1.field1 => table2.field2
        to table1 = {field1 => table2.field2}
        """
        self.table_map_v2 = {}
        for k1, k2 in self.table_map.iteritems():
            t1, f1 = k1.split('.')
            if t1 not in self.table_map_v2:
                self.table_map_v2[t1] = {}
            self.table_map_v2[t1][f1] = k2

    def run_select(self, q, val):
        logging.debug('Select query: ' + q)
        cur = self.read_cursor
        cur.execute(q, (str(val), ))
        fields = map(lambda x:x[0], cur.description)
        return [dict(zip(fields, row)) for row in cur.fetchall()]

    def run_delete(self, table, key, val):
        q = 'DELETE FROM {0} WHERE {1} = %s'.format(table, key)
        logging.debug('Delete query: ' + q)
        cur = self.write_cursor
        if not self.dry_run:
            cur.execute(q, (str(val), ))

    def run_insert(self, table, rows, key):
        if not rows:
            return
        cur = self.write_cursor
        fields = rows[0].keys()
        num_fields = len(fields)

        q = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, ','.join(fields), '%s,' * (num_fields-1) + '%s')
        for row in rows:
            logging.debug('Insert query: ' + q)
            if not self.dry_run:
                cur.execute(q, row.values())

    def run_migration(self, start_key, start_val):
        table, field = start_key.split('.')

        rows = self.run_select('SELECT * from {0} where {1} = %s'.format(table, field), start_val)

        vals = list(set([row[field] for row in rows]))
        for val in vals:
            self.run_delete(table, field, val)

        # Also delete the primary keys to avoid primary key conflicts
        # For now just do 'id' but we should do more introspection
        if len(rows) and 'id' in rows[0].keys():
            ids = list(set([row['id'] for row in rows]))
            for i in ids:
                self.run_delete(table, 'id', val)

        self.run_insert(table, rows, field)

        if table in self.table_map_v2:
            next_steps = self.table_map_v2[table]
            for field, key in next_steps.iteritems():
                vals = list(set(row[field] for row in rows))
                for val in vals:
                    self.run_migration(key, val)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('Please specify start table.key and id')

    START_KEY = sys.argv[1]
    START_VAL = sys.argv[2]

    MIGRATOR = Migrator(config.TABLE_MAP, False)
    MIGRATOR.run_migration(START_KEY, START_VAL)
