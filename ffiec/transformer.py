import datetime
from io import StringIO
import csv
import logging
import json

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
    def mdrm_to_dict(path):
        # from https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm
        types = {
            'J': 'projected',
            'D': 'derived',
            'F': 'reported',
            'R': 'rate',
            'S': 'structure',
            'E': 'examination',
            'P': 'percentage',
        }

        fh = open(path, 'r', encoding='utf-8')
        mdrm_csv = csv.DictReader(StringIO(fh.read()))

        mdrm_hash = {}

        for item in mdrm_csv:
            # there's a top-left cell with PUBLIC in it
            # which throws a wrench in the csv parser, scoop
            # out the data and convert the meneumonic and item_code
            # into an rssd
            mdrm_hash[item['PUBLIC'] + item[None][0]] = item[None]

        for key in mdrm_hash:
            data = mdrm_hash[key]
            if data[0] =='Item Code':
                # there's a header lurking in there
                continue

            mdrm_hash[key] = {
                'meneumonic': item['PUBLIC'],
                'item_code': data[0],
                'start_date': data[1],
                'end_date': data[2],
                'item_name': data[3],
                'confidentiality': data[4],
                'item_type': types[data[5]],
                'reporting_form': data[6],
                'description': data[7],
                'series_glossary': bytes(data[8],'utf-8')
            }

        return mdrm_hash

    @staticmethod
    def to_report__call_report(rssd, period, item, key, mdrm):
        logging.debug('rssd={rssd} period={period} item={item} key={key} mdrm={mdrm}'.format(rssd=rssd, period=period, item=item, key=key, mdrm=mdrm))
        if item[key] is None:
            item[key] = ''
        if isinstance(item[key], bytes):
            item[key] = str(item[key], 'utf-8')
        if isinstance(item[key], (str)):
            item[key] = item[key].strip()
        if isinstance(item[key], (int, float)):
            item[key] = str(item[key])

        row_key = '{rssd}-{period}'.format(rssd=rssd, period=period)
        column_key = 'CallReport:{mdrm}:{key}'.format(mdrm=mdrm, key=key.strip().lower().replace(' ', '_'))

        row = bytes(row_key, 'utf-8')
        column = bytes(column_key, 'utf-8')
        value = bytes(item[key], 'utf-8')

        logging.debug('{row} {column} = {value}'.format(row=row, column=column, value=value))
        return row, column, value

    @staticmethod
    def to_report__institution(rssd, period, report, item_name):
        logging.debug('rssd={rssd} period={period} report={report} item_name={item_name}'.format(rssd=rssd, period=period, report=report, item_name=item_name))
        if report[item_name] is None:
            report[item_name] = ''
        if isinstance(report[item_name], bytes):
            report[item_name] = str(report[item_name])
        if isinstance(report[item_name], (str)):
            report[item_name] = report[item_name].strip()
        if isinstance(report[item_name], (int, float)):
            report[item_name] = str(report[item_name])

        row_key = '{rssd}-{period}'.format(rssd=rssd, period=period)
        column_key = 'Institution:{}'.format(item_name.strip().lower().replace(' ', '_'))

        row = bytes(row_key, 'utf-8')
        column = bytes(column_key, 'utf-8')
        value = report[item_name]

        logging.debug('{row} {col} = {value} '.format(row=row, col=column, value=value))
        return row, column, value

    @staticmethod
    def to_period__institution(period, rssd, document):
        logging.debug('period={period} rssd={rssd} document={document}'.format(period=period, rssd=rssd, document=document))

        value = {}
        # 'document' is a derrived zeep.objects.ReportingFinancialInstitution
        # convert it into a JSON-serializable dict
        if document is None:
            document = {}

        for key in document:
            if isinstance(document[key], str):
                document[key] = document[key].strip()

            value[key] = document[key]

        row = bytes(period, 'utf-8')
        column = bytes('Institution:{}'.format(rssd), 'utf-8')
        value = bytes(json.dumps(value), 'utf-8')

        logging.debug('{row} {col} = {value} '.format(row=row, col=column, value=value))
        return row, column, value

    @staticmethod
    def to_institution__period(period, rssd, document):
        logging.debug('period={period} rssd={rssd} document={document}'.format(period=period, rssd=rssd, document=document))

        value = {}
        # 'document' is a derrived zeep.objects.
        # convert it into a JSON-serializable dict
        if document is None:
            document = {}

        for key in document:
            if isinstance(document[key], str):
                document[key] = document[key].strip()
            value[key] = document[key]

        row = bytes(str(rssd), 'utf-8')
        column = bytes('Period:{}'.format(period), 'utf-8')
        value = bytes(json.dumps(value), 'utf-8')

        logging.debug('{row} {col} = {value} '.format(row=row, col=column, value=value))
        return row, column, value

    @staticmethod
    def to_mdrm__mdrm(mdrm, name, value):
        logging.debug('mdrm={mdrm} key={key} value={value}'.format(mdrm=mdrm, key=name, value=value))

        if value is None:
            value = ''
        if isinstance(value, bytes):
            value = str(value)
        if isinstance(value, (str)):
            value = value.strip()
        if isinstance(value, (int, float)):
            value = str(value)

        row = bytes(mdrm, 'utf-8')
        column = bytes('Metadata:{}'.format(name.strip().lower().replace(' ', '_')), 'utf-8')
        value = bytes(value.strip().replace('\\n', ''), 'utf-8')

        logging.debug('{row} {col} = {value} '.format(row=row, col=column, value=value))
        return row, column, value
