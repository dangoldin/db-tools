#! /usr/bin/env python

import sys
import MySQLdb
import config

class Migrator:
    def __init__(self, table_map):
        self.table_map = table_map
        self.db = MySQLdb.connect(
            host=config.HOST,
            user=config.USER,
            passwd=config.PASSWORD,
            db=config.DB)
        self.parse_map()

    def __del__(self):
        if self.db:
            try:
                self.db.close()
            except Exception, e:
                pass

    def parse_map(self):
        # Go from table1.field1 => table2.field2
        # to table1 = {field1 => table2.field2}
        self.table_map_v2 = {}
        for k1, k2 in self.table_map.iteritems():
            t1, f1 = k1.split('.')
            if t1 not in self.table_map_v2:
                self.table_map_v2[t1] = {}
            self.table_map_v2[t1][f1] = k2

    def run_select(self, q):
        cur = self.db.cursor()
        cur.execute(q)
        print q
        fields = map(lambda x:x[0], cur.description)
        return [dict(zip(fields, row)) for row in cur.fetchall()]

    def run_delete(self, table, key, val):
        cur = self.db.cursor()
        q = 'DELETE FROM {0} WHERE {1} = "{2}"'.format(table, key, val)
        # cur.execute(q)
        print q

    def run_insert(self, table, rows, key):
        cur = self.db.cursor()
        for row in rows:
            # Insert new data
            vals = ','.join('"{0}"'.format(str(x)) for x in row.values())
            # Hack for now to deal with nulls
            vals = vals.replace('"None"', 'NULL')
            q = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, ','.join(row.keys()), vals)
            # cur.execute(q)
            print q

    def run_migration(self, start_key, start_val):
        table, field = start_key.split('.')

        rows = self.run_select('SELECT * from {0} where {1} = "{2}"'.format(table, field, str(start_val)))

        vals = list(set([row[field] for row in rows]))
        for val in vals:
            self.run_delete(table, field, val)

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
        exit(1)

    start_key = sys.argv[1]
    start_val = sys.argv[2]

    m = Migrator(config.TABLE_MAP)
    m.run_migration(start_key, start_val)
