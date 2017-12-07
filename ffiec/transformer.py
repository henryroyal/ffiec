import datetime
from io import StringIO
import csv
import logging

class Transformer:

    @staticmethod
    def normalize_mdrm(mdrm):
        if not mdrm:
            return None
        return mdrm.upper()

    @staticmethod
    def bytes_to_unicode(response_bytes):
        return str(response_bytes, 'utf-8')

    @staticmethod
    def report_period_to_datetime(period):
        period += ' -0400'  # FFIEC periods are EST-0400
        return datetime.datetime.strptime(period, '%m/%d/%Y %z')

    @staticmethod
    def sdf_to_dictreader(sdf):
        return csv.DictReader(StringIO(sdf), delimiter=';')

    @staticmethod
    def csv_to_dictreader(csvdata):
        return csv.DictReader(StringIO(csvdata))

    @staticmethod
    def to_report__call_report(rssd, period, item, key, mdrm):
        if not item[key]:
            item[key] = ''
        if isinstance(item[key], bytes):
            item[key] = str(item[key], 'utf-8')
        if isinstance(item[key], (str)):
            item[key] = item[key].strip()
        if isinstance(item[key], (int, float)):
            item[key] = str(item[key])

        row = bytes('{rssd}-{period}'.format(rssd=rssd, period=period), 'utf-8')
        column = bytes('CallReport:{mdrm}:{key}'.format(mdrm=mdrm, key=key.strip().lower().replace(' ', '_')), 'utf-8')
        value = bytes(item[key], 'utf-8')

        logging.debug('{row} {column} = {value}'.format(row=row,
                                                        column=column,
                                                        value=value))
        return row, column, value

    @staticmethod
    def to_report__institution(rssd, period, institution, key):
        if not institution[key]:
            institution[key] = ''
        if isinstance(institution[key], bytes):
            institution[key] = str(institution[key])
        if isinstance(institution[key], (str)):
            institution[key] = institution[key].strip()
        if isinstance(institution[key], (int, float)):
            institution[key] = str(institution[key])

        row = bytes('{rssd}-{period}'.format(rssd=rssd, period=period), 'utf-8')
        column = bytes('Institution:{}'.format(key.strip().lower().replace(' ', '_')), 'utf-8')
        value = institution[key]

        logging.debug('{row} {col} = {value} '.format(row=row,
                                                      col=column,
                                                      value=value))
        return row, column, value

    @staticmethod
    def to_period__report_period():
        return

    @staticmethod
    def to_institution__institution():
        return

    @staticmethod
    def to_mdrm__mdrm():
        return
