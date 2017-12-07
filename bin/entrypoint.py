import sys
import click
import json_log_formatter
import logging

from ffiec.extractor import Extractor
from ffiec.transformer import Transformer
from ffiec.loader import Loader

LOG_LEVELS = click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
LOG_FORMATS = click.Choice(['LINE', 'JSON'])

FFIEC_WSDL = 'https://cdr.ffiec.gov/Public/PWS/WebServices/RetrievalService.asmx?WSDL'
MDRM = 'MDRM #'
CALL_REPORT = 'Call'
SDF = 'SDF'
ID_RSSD = 'ID_RSSD'
RSSD_WILDCARD = 11111111111  # wildcard since RSSD identifier must 10 digits or less

REPORT_TABLE = 'report'
REPORT_TABLE_DEFINITION = {'CallReport': dict(), 'Institution': dict()}

PERIOD_TABLE = 'period'
PERIOD_TABLE_DEFINITION = {'ReportPeriod': dict()}

INSTITUTION_TABLE = 'institution'
INSTITUTION_TABLE_DEFINITION = {'Institution': dict()}

MDRM_TABLE = 'mdrm'
MDRM_TABLE_DEFINITION = {'MDRM': dict()}


def init_logging(logging_level, logging_format):
    logger = logging.getLogger('etlFFIEC')
    logger.setLevel(logging_level)

    log_handler = logging.StreamHandler()
    log_handler.setLevel(logging_level)

    if logging_format == 'LINE':
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    elif logging_format == 'JSON':
        formatter = json_log_formatter.JSONFormatter()

    else:
        raise ValueError('unknown log format, something has gone terribly wrong')

    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)


@click.command()
@click.argument('target_rssd')
@click.option('--hbase-reinit', envvar='HBASE_REINIT', is_flag=True, type=bool)
@click.option('--hbase-host', envvar='HBASE_HOST', default='127.0.0.1')
@click.option('--hbase-port', envvar='HBASE_PORT', default=9090)
@click.option('--ffiec-wsdl-url', envvar='FFIEC_WSDL_URL', default=FFIEC_WSDL)
@click.option('--ffiec-username', envvar='FFIEC_USERNAME', required=True)
@click.option('--ffiec-token', envvar='FFIEC_TOKEN', required=True)
@click.option('--logging-level', envvar='LOGGING_LEVEL', type=LOG_LEVELS, default='WARNING')
@click.option('--logging-format', envvar='LOGGING_FORMAT', type=LOG_FORMATS, default='LINE')
def main(target_rssd, hbase_reinit, hbase_host, hbase_port,
         ffiec_wsdl_url, ffiec_username, ffiec_token,
         logging_level, logging_format):

    if target_rssd == '*':
        logging.warning('collecting for all rssd values'.format(target_rssd))
        target_rssd = RSSD_WILDCARD
    else:
        logging.info('filtering for institution rssd {}'.format(target_rssd))
        target_rssd = int(target_rssd)

    init_logging(logging_level, logging_format)
    logging.debug('initialized logging')

    loader = Loader(hbase_host)
    hbase = loader.get_connection()
    if hbase_reinit:
        try:
            for table in (REPORT_TABLE, PERIOD_TABLE, INSTITUTION_TABLE, MDRM):
                hbase.disable_table(table)
                hbase.delete_table(table)
                logging.warning('deleted hbase table {}'.format(table))
        except Exception as err:  #FIXME build thrift objects and make this correct
            logging.critical(str(err))

        # TODO tune this
        hbase.create_table(REPORT_TABLE, REPORT_TABLE_DEFINITION)
        hbase.create_table(PERIOD_TABLE, PERIOD_TABLE_DEFINITION)
        hbase.create_table(INSTITUTION_TABLE, INSTITUTION_TABLE_DEFINITION)
        hbase.create_table(MDRM_TABLE, MDRM_TABLE_DEFINITION)

    reports_table = hbase.table(REPORT_TABLE)
    periods_table = hbase.table(PERIOD_TABLE)
    institutions_table = hbase.table(INSTITUTION_TABLE)
    mdrm_table = hbase.table(MDRM_TABLE)

    #TODO populate mdrm table
    #mdrm_fh = open('MDRM.csv', 'r')
    #mdrm_csv = mdrm_fh.read()
    #mdrm = Transformer.csv_to_dictreader(mdrm_csv)
    #logging.info('read MDRM data')

    extractor = Extractor(ffiec_wsdl_url, ffiec_username, ffiec_token)
    extractor.setup()

    for period in extractor.reporting_periods_by_series(CALL_REPORT):
        reporters = extractor.client.service.RetrievePanelOfReporters(CALL_REPORT, period) #TODO tuck this in extractor
        logging.info('retrieved {len} reporters for panel {period}'.format(len=len(reporters), period=period))

        rssd_set = {int(reporter[ID_RSSD]) for reporter in reporters}
        if target_rssd not in rssd_set:
            logging.warning('{target} not in rssd set for period {date}'.format(target=target_rssd, date=period))
            continue

        for institution in reporters:
            rssd = int(institution[ID_RSSD])
            if rssd != target_rssd and target_rssd != RSSD_WILDCARD:
                continue

            response = Transformer.bytes_to_unicode(
                extractor.client.service.RetrieveFacsimile(CALL_REPORT, period, ID_RSSD, institution[ID_RSSD], SDF)
            )

            facsimile = Transformer.sdf_to_dictreader(response)
            row_key = bytes('{rssd}-{period}'.format(rssd=rssd, period=period), 'utf-8')

            for key in institution:
                if not institution[key]:
                    institution[key] = ''
                if isinstance(institution[key], bytes):
                    institution[key] = str(institution[key])
                if isinstance(institution[key], (str)):
                    institution[key] = institution[key].strip()
                if isinstance(institution[key], (int, float)):
                    institution[key] = str(institution[key])

                column_key = bytes('Institution:{}'.format(key.strip().lower().replace(' ', '_')), 'utf-8')
                logging.debug('{row} Institution:{col} = {value} '.format(value=institution[key],
                                                                         col=column_key,
                                                                         row=period))
                reports_table.put(row_key, {column_key: institution[key]})

            for item in facsimile:
                formatted_mdrm = item[MDRM].upper()
                for key in item:
                    formatted_key = key.strip().lower().replace(' ', '_')
                    if not item[key]:
                        item[key] = ''
                    if isinstance(item[key], bytes):
                        item[key] = str(item[key], 'utf-8')
                    if isinstance(item[key], (str)):
                        item[key] = item[key].strip()
                    if isinstance(item[key], (int, float)):
                        item[key] = str(item[key])

                    formatted_value = bytes(item[key], 'utf-8')
                    column_key = bytes('CallReport:{mdrm}:{key}'.format(mdrm=formatted_mdrm,
                                                                        key=formatted_key), 'utf-8')

                    logging.debug('{row} CallReport:{mdrm}:{key} = {value}'.format(row=row_key,
                                                                                  mdrm=item[MDRM],
                                                                                  key=formatted_key,
                                                                                  value=formatted_value))
                    reports_table.put(row_key, {column_key: formatted_value})

    logging.info('job complete')
    sys.exit(0)

if __name__ == '__main__':
    main()
