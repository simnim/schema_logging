#!/usr/bin/env python
import json
#import mysql.connector
import pandas as pd
import os
from sqlalchemy import create_engine


"""
We've got some legacy tables named like *.database_info that we'd like to capture in the logging.
Let's grab those tables.
"""

def dump_database_infos(dir_prefix, engine):
    db_info_schemas = pd.read_sql_query(
                          """ SELECT table_schema
                              from information_schema.tables
                              where table_name = 'database_info';
                          """, engine)

    for schema in db_info_schemas['table_schema']:
        db_info = pd.read_sql_table('database_info', engine, schema=schema )
        db_info_j = json.loads(db_info.to_json(orient='records'))
        db_info_j.sort(key=lambda r: r['info_id'])
        if not os.path.isdir('%s/%s'%(dir_prefix,schema)):
            os.makedirs('%s/%s'%(dir_prefix,schema))
        with open('%s/%s/database_info.json'%(dir_prefix, schema), 'w') as dbi_f:
            dbi_f.write(json.dumps(db_info_j, indent=4))

if __name__ == "__main__":
    CONFIG_FILE_LOC = os.path.expanduser("~/schema_logger_config.json")
    config_json = json.loads(open(CONFIG_FILE_LOC).read())
    engine = create_engine('mysql://%s:%s@%s/' %(
                        config_json['username'],
                        config_json['password'],
                        config_json['host'],
                    ), pool_recycle=3600)
    dir_prefix = os.path.dirname(os.path.abspath(__file__))
    dump_database_infos(dir_prefix + '/database_infos', engine)
