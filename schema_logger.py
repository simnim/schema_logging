#!/usr/bin/env python
"""
Nick Hahner 2017
https://github.com/simnim/schema_logging
"""

from sqlalchemy import create_engine
import pandas as pd
import os
import errno
from datetime import datetime
from tempfile import NamedTemporaryFile
import subprocess as sp
import shutil
import json
import re

this_script_path = os.path.dirname( os.path.realpath( __file__ ) )
#os.chdir(this_script_path)

# I nabbed this function from http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def mkdir_p(path):
    """ 'mkdir -p' in Python """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise



config_json = json.loads(open(this_script_path + "/config.json").read())

DEBUG = config_json.get("DEBUG",'False').lower() in ['t', 'true', 'yes', 'y']

# rstrip('/') + '/' make sure it ends in exactly one /
SQL_DIR = config_json.get("SQL_DIR", this_script_path + '/sql_files').rstrip('/') + '/'

engine = create_engine('mysql://%s:%s@%s/' %(
                    config_json['username'],
                    config_json['password'],
                    config_json['host'],
                ), pool_recycle=3600)



def get_table_sql(schema, table_name):
    # FIXME: For some reason accessing engine directly seems to be more fragile, I'll stop using pandas for this once I poke at it a bit more.
    #return engine.execute("show create table %s;"%table_name, engine)
    if DEBUG:
        print "get_table_sql", schema, table_name
    table_sql = pd.read_sql_query("show create table `%s`.`%s`;"%(schema, table_name), engine).ix[0,1] + ';\n'
    # Let's delete the auto increment counter part. It'll make diffs break when rows are added.
    return re.sub('AUTO_INCREMENT=\d+ ', '', table_sql)

def get_func_sql(schema, func_type, func_name):
    if DEBUG:
        print "get_func_sql", schema, func_type, func_name
    return pd.read_sql_query("show create %s `%s`.`%s`;"%(func_type, schema, func_name), engine).ix[0,2] + ';\n'

def get_view_sql(schema, view_name):
    if DEBUG:
        print "get_view_sql", schema, view_name
    return pd.read_sql_query("show create view `%s`.`%s`;"%(schema, view_name), engine).ix[0,1] + ';\n'




def dump_records_to_temp():
    """ Dumps out the db schemas to a directory and returns the name of said dir """
    timestamp = str(datetime.now()).replace(" ", "__")
    parent_dir = SQL_DIR + 'temp'

    # Write out the table object files.
    table_info_df = pd.read_sql_query(
                """ SELECT * from information_schema.tables
                    where table_schema not in
                            ('information_schema',
                             'mysql',
                             'performance_schema',
                             'sys')
                     AND TABLE_TYPE = 'BASE TABLE';
                    """, engine)
    for idx, table in table_info_df.iterrows():
        sql_file_parent = parent_dir + '/tables/' + table['TABLE_SCHEMA']
        mkdir_p(sql_file_parent)
        open(sql_file_parent + '/' + table['TABLE_NAME'] + '.sql', 'w')\
            .write(get_table_sql(table['TABLE_SCHEMA'], table['TABLE_NAME']))


    # Write out the view object files
    view_info_df = pd.read_sql_query(
                """ SELECT * from information_schema.views
                    where table_schema not in
                            ('information_schema',
                             'mysql',
                             'performance_schema',
                             'sys')
                    ;
                    """, engine)
    for idx, view in view_info_df.iterrows():
        sql_file_parent = parent_dir + '/views/' + view['TABLE_SCHEMA']
        mkdir_p(sql_file_parent)
        open(sql_file_parent + '/' + view['TABLE_NAME'] + '.sql', 'w')\
            .write(get_view_sql(view['TABLE_SCHEMA'], view['TABLE_NAME']))


    # Write out the function object files
    # Grab both the name and {function, procedure} from INFORMATION_SCHEMA.ROUTINES
    # Then call 'show create {function | procedure} ___'
    # Then dump the create column's contents to a file
    function_info_df = pd.read_sql_query(
                """ SELECT * from INFORMATION_SCHEMA.ROUTINES
                    where ROUTINE_SCHEMA not in
                            ('information_schema',
                             'mysql',
                             'performance_schema',
                             'sys');
                    """, engine)
    for idx, function in function_info_df.iterrows():
        sql_file_parent = parent_dir + '/functions/' + function['ROUTINE_SCHEMA']
        mkdir_p(sql_file_parent)
        open(sql_file_parent + '/' + function['ROUTINE_NAME'] + '.sql', 'w')\
            .write(get_func_sql(function['ROUTINE_SCHEMA'], function['ROUTINE_TYPE'], function['ROUTINE_NAME']))

    # Wrtie out the mat view objects #NOTIMPLEMENTED I don't need this for mysql, just postgres!

    return timestamp


def dump_and_archive():
    """
    Dumps the latest versions of the schemas from the db
    It then figures out what files have changed so it can
    intelligently create a directory for the timestamp of the db dump
    and makes SQL files for each db object. If an object has not
    changed this time around then it hard links it to previous one to save space.
    If nothing changed from the previous time then it just makes
    a symlink to the last dump with any changes. (Mainly so you know it checkd.)

    This function should be run periodically with CRON to watch a DB for
    changes and to log schema changes as they happen.

    Bonus: Optionally sends the diff to a Slack channel if there are diffs.
    """
    # First figure out if there's a 'current' sql dir, aka we've run this before.
    current_path = SQL_DIR+'current'
    prev_path = SQL_DIR+'previous'
    temp_dump_dir = SQL_DIR + 'temp/'
    timestamp = dump_records_to_temp()
    new_dir = SQL_DIR + timestamp

    # If the 'current' symlink is present then we know we should do the rsync trick.
    if os.path.exists(current_path):
    #     Let's pull out the previous one's timestamp
    #     prev_timestamp = os.readlink(current_path)

        diff_prev_to_this = sp.check_output("git diff --no-index %s/ %s/ || TRUE" % (current_path, temp_dump_dir), shell=True)
        if DEBUG:
            print diff_prev_to_this
        if diff_prev_to_this.strip() == '':
            # It's the same as last time.
            # No need to do anything fancy, let's just make this dump a symlink to the last identical dump.
            os.symlink(os.readlink(current_path), './' + new_dir)
        else:
            # Now let's use the rsync trick to hardlink our new SQL to the previous SQL.
            # This way any unchanged files just get another hard link to an existing file.
            # This saves a ton of resources as only the files with changes are actually created
            # The cool part is that the new directory acts like it has all the files, because it does!
            # For now lets just wrap the rsync command instead of re-implementing the wheel.
            # rsync -a --link-dest=$PREV_SBACKUP $SOURCE $NEW_DIR
            rsync_out = sp.check_output("rsync -acvv --no-times --link-dest=%s %s %s" % (
                                    current_path,
                                    emp_dump_dir,
                                    new_dir
                                ),
                           shell = True
                         )

            # rsync will enumerate every file to send.
            if DEBUG:
                print rsync_out

            # Now let's move current to previous
            # First delete the old 'previous'
            if os.path.exists(prev_path):
                os.remove(prev_path)
            # Move the existing 'current' to 'previous'
            os.rename(current_path, prev_path)
            os.symlink(timestamp, './' + current_path)

        # Either way we want to clear the temp dump
        shutil.rmtree(temp_dump_dir)

    else:
        # Move the temp dump dir to the new directory location.
        os.rename(temp_dump_dir, new_dir)
        os.symlink(timestamp, './' + current_path)


if __name__ == "__main__":
    dump_and_archive()
