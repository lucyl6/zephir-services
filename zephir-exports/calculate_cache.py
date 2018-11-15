#!/usr/bin/env python

import datetime
import os
import socket
import zlib

from environs import Env
import json
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import yaml

from lib.export_cache import ExportCache
from lib.utils  import zephir_config
from lib.vufind_formatter import VufindFormatter
import lib.export_selection as es

print(datetime.datetime.time(datetime.datetime.now()))

# APPLICATION SETUP
# load environment
env = Env()
env.read_env()

# load configuration files
config = zephir_config(env("ZEPHIR_ENV", socket.gethostname()).lower(), os.path.join(os.path.dirname(__file__),"config"))


def main():
    htmm_db = config["database"][config["env"]]

    HTMM_DB_CONNECT_STR = str(
        URL(
            htmm_db.get("drivername", None),
            htmm_db.get("username", None),
            htmm_db.get("password", None),
            htmm_db.get("host", None),
            htmm_db.get("port", None),
            htmm_db.get("database", None),
        )
    )
    htmm_engine = create_engine(HTMM_DB_CONNECT_STR)
    # live_statement = "select cid, db_updated_at, zr.id as htid, var_usfeddoc, var_score, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id and attr_ingest_date is not null and cid = \"{}\" order by var_usfeddoc, var_score"
    #feddoc_statement = "select cid, db_updated_at, zr.id as htid, var_usfeddoc, var_score, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id and attr_ingest_date is not null order by cid, var_usfeddoc, var_score limit 30"
    #orig_stmt = "select cid, db_updated_at, zr.id as htid, var_score, concat(cid,'_',zr.autoid) as vufind_sort, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id where attr_ingest_date is not null order by cid, var_score DESC, vufind_sort ASC"
    start_time = datetime.datetime.time(datetime.datetime.now())

    cache = ExportCache(os.path.abspath("cache"),'complete-gz')

    select_class = getattr(es, 'VufindOriginal')

    try:
        bulk_session = cache.session()
        conn = mysql.connector.connect(
            user=htmm_db.get("username", None),
            password=htmm_db.get("password", None),
            host=htmm_db.get("host", None),
            database=htmm_db.get("database", None))

        cursor = conn.cursor(named_tuple=True)
        cursor.execute(select_class.sql_query())

        current_record =  None
        current_group = []
        bulk_entries = []

        for idx, row in enumerate(cursor):
            new_record = select_class(row)
            if current_group and new_record.group_id != current_group[0].group_id:
                # prepare cache of last group
                cache_params = select_class.create_cache_params(VufindFormatter,current_group)
                entry = cache.create_entry(**cache_params)
                bulk_entries.append(entry)


                # save to cache database (in bulk)
                if idx % 5000 == 0:
                    bulk_session.bulk_save_objects(bulk_entries)
                    bulk_entries = []

                # reset for next group
                current_group = []

            # add item
            current_group.append(new_record)

        # add record information to current grouping
            #if last_cache_entry and bulk_entries and bulk_entries[-1].cache_id != last_cache_entry.cache_id:
        cache_params = select_class.create_cache_params(VufindFormatter,current_group)

        entry = cache.create_entry(**cache_params)
        if bulk_entries and bulk_entries[-1].cache_id != entry.cache_id:
            bulk_entries.append(entry)

        if bulk_entries:
            bulk_session.bulk_save_objects(bulk_entries)

        bulk_session.commit()
        bulk_session.close()

        print("start:{}".format(start_time))
        print(datetime.datetime.time(datetime.datetime.now()))
    finally:
        cursor.close()
        conn.close()

def prepare_cache_entry(cache, current):
        prioritized = VufindSelect.prioritize(current['records'])
        formatted_record = VufindFormatter.create_record(prioritized)

        cache_id = current['group_id']
        cache_data = json.dumps(formatted_record.as_dict(),separators=(',',':'))
        cache_date = max(current['dates'])
        cache_key = VufindSelect.generate_key(len(current['records']), cache_date)

        entry = cache.create_entry(cache_id, cache_key, cache_data, cache_date)

if __name__ == "__main__":
    main()
