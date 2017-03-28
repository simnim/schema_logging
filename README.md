# schema_logging
Make it easy to log the definitions of all your tables, views, and functions.

Currently this project only works for MySQL.
I intend to add functionality for Postgres and others in the future.

## Prereqs:
`brew install rsync git`

`pip install sqlalchemy pandas`

## Setup:
Create a json config file (named `~/schema_logger_config.json`) in your home directory that has the following fields "username", "password", "host", "SQL_DIR" (optional, it's where we'll put the dumped files)

example file:
```
{
  "username": "best_name",
  "password": "Password1",
  "host": "localhost",
  "SQL_DIR": "~/my_sql_files"
}
```

### Highly recommended:
Add a CRON job that runs this script hourly on your database server.

## How can this be useful????
If you want to figure out how the definitions of various objects changed then you can run (for example) `git diff --no-index sql_files/previous/ sql_files/current/` Feel free to substitute any directory from sql_files in the diff. This enables comparisons across time points.

## What this repo actually does:
It grabs the database object definitions from the database and creates files in the filesystem to represent said objects. This makes it easy to use your favorite unix tools to inspect the definitions of files. eg `cat sql_files/current/tables/TCGA/mutations.sql`.

## What this repo does NOT do:
If you edit sql files then nothing happens to the DB. It's just a dumb logging tool.

The repo tells you nothing about who changed what, that's for you to figure out.

## Future directions:
* Add tests
* Add support for Postgres
* Add support for multiple db hosts
* Add support for collecting stats on row counts for tables (maybe just for tables with indexes to make it fast.)
