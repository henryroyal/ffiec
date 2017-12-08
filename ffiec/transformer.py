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
        logging.debug(
            'rssd={rssd} period={period} item={item} key={key} mdrm={mdrm}'.format(rssd=rssd,
                                                                                   period=period,
                                                                                   item=item,
                                                                                   key=key,
                                                                                   mdrm=mdrm))
        if item[key] is None:
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
        logging.debug('rssd={rssd} period={period} institution={institution} key={key}'.format(rssd=rssd,
                                                                                              period=period,
                                                                                              institution=institution,
                                                                                              key=key))
        if institution[key] is None:
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
    def to_mdrm__mdrm(mdrm, key, value):
        logging.debug('mdrm={mdrm} key={key} value={value}'.format(mdrm=mdrm, key=key, value=value))

        if value is None:
            value = ''
        if isinstance(value, bytes):
            value = str(value)
        if isinstance(value, (str)):
            value = value.strip()
        if isinstance(value, (int, float)):
            value = str(value)

        row = bytes(mdrm, 'utf-8')
        column = bytes('Metadata:{}'.format(key.strip().lower().replace(' ', '_')), 'utf-8')
        value = bytes(value.strip().replace('\\n', ''), 'utf-8')

        logging.debug('{row} {col} = {value} '.format(row=row, col=column, value=value))
        return row, column, value
