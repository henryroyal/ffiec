import datetime
from io import StringIO
import csv


class Transformer:

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
