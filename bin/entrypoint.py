import sys
import click
import json_log_formatter
import logging
from datetime import datetime

from ffiec.extractor import Extractor
from ffiec.transformer import Transformer
from ffiec.loader import Loader

LOG_LEVELS = click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
LOG_FORMATS = click.Choice(['LINE', 'JSON'])

FFIEC_WSDL = 'https://cdr.ffiec.gov/Public/PWS/WebServices/RetrievalService.asmx?WSDL'
MDRM = 'MDRM #'
ID_RSSD = 'ID_RSSD'
RSSD_WILDCARD = 11111111111  #RSSD identifier must 10 digits or less


def init_logging(logging_level, logging_format):
    logger = logging.getLogger()
    logger.setLevel(logging_level)

    if logging_format == 'LINE':
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    elif logging_format == 'JSON':
        formatter = json_log_formatter.JSONFormatter()
    else:
        raise ValueError('unknown log format, something has gone terribly wrong')

    lh = logging.StreamHandler()
    lh.setFormatter(formatter)
    logger.addHandler(lh)


@click.command()
@click.argument('target_rssd')
@click.option('--hbase-reinit', envvar='HBASE_REINIT', is_flag=True, type=bool)
@click.option('--hbase-host', envvar='HBASE_HOST', default='127.0.0.1')
@click.option('--hbase-port', envvar='HBASE_PORT', default=9090)
@click.option('--ffiec-wsdl-url', envvar='FFIEC_WSDL_URL', default=FFIEC_WSDL)
@click.option('--ffiec-username', envvar='FFIEC_USERNAME', required=True)
@click.option('--ffiec-token', envvar='FFIEC_TOKEN', required=True)
@click.option('--mdrm-path', envvar='MDRM_PATH', default='MDRM.csv')
@click.option('--logging-level', envvar='LOGGING_LEVEL', type=LOG_LEVELS, default='WARNING')
@click.option('--logging-format', envvar='LOGGING_FORMAT', type=LOG_FORMATS, default='LINE')
def main(target_rssd, hbase_reinit, hbase_host, hbase_port,
         ffiec_wsdl_url, ffiec_username, ffiec_token,
         mdrm_path, logging_level, logging_format):

    job_start_timestamp = datetime.now()
    init_logging(logging_level, logging_format)
    logging.warning('initialized logging')

    if target_rssd == '*':
        target_rssd = RSSD_WILDCARD
    else:
        target_rssd = int(target_rssd)

    loader = Loader(hbase_host)
    loader.connect()
    if hbase_reinit:
        loader.delete_tables()
        loader.create_tables()

    mdrm_fh = open(mdrm_path, 'r')
    mdrm_csv = mdrm_fh.read()
    mdrm = Transformer.csv_to_dictreader(mdrm_csv)
    logging.info('read MDRM data')
    #TODO populate mdrm table

    ffiec = Extractor(ffiec_wsdl_url, ffiec_username, ffiec_token)
    ffiec.setup()

    for period in ffiec.reporting_periods():
        reporters = ffiec.reporting_institutions(period)
        count_of_reporters = len(reporters)
        #TODO - populate reporter table

        rssd_set = {int(reporter[ID_RSSD]) for reporter in reporters}
        if target_rssd not in rssd_set and target_rssd != RSSD_WILDCARD:
            logging.info('{target} not in rssd set for period {date}'.format(target=target_rssd, date=period))
            continue

        for institution in reporters:
            with loader.report_table.batch() as report_table:
                #TODO populate institution table
                rssd = int(institution[ID_RSSD])
                if rssd != target_rssd and target_rssd != RSSD_WILDCARD:
                    continue

                unicode_sdf_facsimile = Transformer.bytes_to_unicode(
                    ffiec.call_report_facsimile(period, institution)
                )
                facsimile = Transformer.sdf_to_dictreader(unicode_sdf_facsimile)

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

                    report_table.put(row_key, {column_key: institution[key]})

                logging.info('loaded RSSD# {rssd} from {period} into report::Institution'.format(rssd=rssd,
                                                                                                 period=period))

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

                        report_table.put(row_key, {column_key: formatted_value})

                logging.info('loaded RSSD# {rssd} from {period} into report::CallReport'.format(rssd=rssd,
                                                                                                period=period))
        logging.info('job complete')
        job_complete_timestamp = datetime.now()
        job_runtime = (job_complete_timestamp - job_start_timestamp)
        logging.info('runtime: {}'.format(job_runtime))

if __name__ == '__main__':
    main()
    sys.exit(0)