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

    def __del__(self):
        if self.db:
            try:
                self.db.close()
            except Exception, e:
                pass

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
            # Wipe data before hand
            self.run_delete(table, key, row[key])
            # Insert new data
            vals = ','.join('"{0}"'.format(str(x)) for x in row.values())
            q = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, ','.join(row.keys()), vals)
            # cur.execute(q)
            print q

    def run_migration(self, start_key, start_val):
        if start_key not in self.table_map:
            print 'Unable to find map info for ' + start_key + '.. stoping'
            return

        table, field = start_key.split('.')

        rows = self.run_select('SELECT * from {0} where {1} = "{2}"'.format(table, field, str(start_val)))

        vals = [row[field] for row in rows]

        self.run_insert(table, rows, field)

        next_key = self.table_map[start_key]
        for val in vals:
            self.run_migration(next_key, val)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception('Please specify start table.key and id')
        exit(1)

    start_key = sys.argv[1]
    start_val = sys.argv[2]

    m = Migrator(config.TABLE_MAP)
    m.run_migration(start_key, start_val)
