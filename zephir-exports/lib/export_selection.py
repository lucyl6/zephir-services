import json
import zlib

class VufindOriginal:

    @staticmethod
    def sql_query():
        return "select cid, metadata_json as metadata, db_updated_at as db_date, zr.id as htid, var_score, concat(cid,'_',zr.autoid) as cid_autoid_concat from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id where attr_ingest_date is not null order by cid, var_score DESC, cid_autoid_concat ASC"

    @staticmethod
    def generate_key(num_records, max_date):
        return zlib.crc32("{}{}".format(num_records, max_date).encode('utf8'))

    @staticmethod
    def convert_data(data):
        return json.dumps(data.as_dict(),separators=(',',':'))

    @staticmethod
    def prepare(data):
        records = []
        dates = []
        no_scores = True
        for row in data:
            records.append(row.record)
            dates.append(row.date)
            if row.score:
                no_scores = False
        if no_scores:
            records.reverse()
        date = max(dates)
        format_param = {"records": records, "control_num": data[0].group_id}
        cache_param = {"cache_id":data[0].group_id, "cache_key":VufindOriginal.generate_key(len(records),date), "cache_date" :date }
        return format_param, cache_param

    @staticmethod
    def create_cache_params(formatter,data):
        format_params, cache_params = VufindOriginal.prepare(data)
        formatted_record = formatter.create_record(**format_params)
        converted_data = VufindOriginal.convert_data(formatted_record)
        cache_params["cache_data"] = converted_data
        return cache_params


    def __init__(self, data):
        self._data = data
        self.group_id = self._data[0]
        self.record = self._data[1]
        self.date = self._data[2]
        self.score = self._data[4]
