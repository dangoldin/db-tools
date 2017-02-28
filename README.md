# db-tools

A series of small and hopefully useful database utilities.

## migrate.py
A quick script to allow you to migrate data from one database to another. You are required to specify the key mapping so the script knows how one table depends on another but after that the script should work it's way up to migrate each of the dependent and referenced rows.

ToDos:
- Support other DBs (currently it's MySQL)
- Deal with schema differences
- Handle uniqueness constraint conflicts
