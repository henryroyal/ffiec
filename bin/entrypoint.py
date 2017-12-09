import sys
import click
import json_log_formatter
import logging
from datetime import datetime

from ffiec.extractor import Extractor
from ffiec.transformer import Transformer
from ffiec.hbase import Hbase

LOG_LEVELS = click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
LOG_FORMATS = click.Choice(['LINE', 'JSON'])

FFIEC_WSDL = 'https://cdr.ffiec.gov/Public/PWS/WebServices/RetrievalService.asmx?WSDL'
MDRM = 'MDRM #'

ID_RSSD = 'ID_RSSD'
RSSD_WILDCARD = 11111111111  # RSSD identifier must 10 digits or less


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


def current_runtime(start_time):
    duration = (datetime.now() - start_time)
    return 'current runtime: {time}'.format(time=duration)


def rssd_is_filtered(rssd_target, rssd):
    if rssd_target == RSSD_WILDCARD or int(rssd) == rssd_target:
        logging.debug('rssd {rssd} passed filter'.format(rssd=rssd))
        return False

    return True


def period_is_filtered(period_target, period):
    if not period_target or period_target == period:
        logging.debug('period {time} passed filter'.format(time=period))
        return False

    return True

def init_database(hbase):
    hbase.create_metadata_tables()
    hbase.create_report_table()


def truncate_database(hbase):
    hbase.delete_metadata_tables()
    hbase.delete_report_table()
    hbase.create_metadata_tables()
    hbase.create_report_table()


def load_mdrm_metadata(hbase, mdrm_path):
    mdrm_data = Transformer.mdrm_to_dict(mdrm_path)
    data_dictionary = hbase.data_dictionary_table.batch()
    for mdrm, item in mdrm_data.items():
        if not isinstance(item, dict):
            continue

        mdrm = Transformer.normalize_mdrm(mdrm)
        for key, value in item.items():
            row_key, column_key, value = Transformer.to_mdrm__mdrm(mdrm, key, value)
            data_dictionary.put(row_key, {column_key: value})
    data_dictionary.send()


@click.command()
@click.option('--init', envvar='INIT', is_flag=True, type=bool)
@click.option('--truncate-tables', envvar='TRUNCATE_TABLES', is_flag=True, type=bool)
@click.option('--update-metadata', envvar='UPDATE_METADATA', is_flag=True, type=bool)
@click.option('--rssd-target', envvar='RSSD_TARGET', default=None)
@click.option('--period-target', envvar='PERIOD_TARGET', default=None)
@click.option('--hbase-host', envvar='HBASE_HOST', default='127.0.0.1')
@click.option('--ffiec-wsdl-url', envvar='FFIEC_WSDL_URL', default=FFIEC_WSDL)
@click.option('--ffiec-username', envvar='FFIEC_USERNAME')
@click.option('--ffiec-token', envvar='FFIEC_TOKEN')
@click.option('--mdrm-path', envvar='MDRM_PATH', default='MDRM.csv')
@click.option('--logging-level', envvar='LOGGING_LEVEL', type=LOG_LEVELS, default='WARNING')
@click.option('--logging-format', envvar='LOGGING_FORMAT', type=LOG_FORMATS, default='LINE')
def main(init, truncate_tables, update_metadata, rssd_target, period_target,
         hbase_host, ffiec_wsdl_url, ffiec_username, ffiec_token,
         mdrm_path, logging_level, logging_format):

    job_start_timestamp = datetime.now()
    init_logging(logging_level, logging_format)
    logging.debug('initialized logging')

    hbase = Hbase(hbase_host)
    hbase.connect()

    if init:
        init_database(hbase)
        load_mdrm_metadata(hbase, mdrm_path)
        logging.info(current_runtime(job_start_timestamp))
        logging.critical('created all tables, exiting...')
        sys.exit(0)

    if truncate_tables:
        truncate_database(hbase)
        logging.info(current_runtime(job_start_timestamp))
        logging.critical('finished truncating tables, exiting...')
        sys.exit(0)

    if update_metadata:
        # load the Fed's Micro Data Reference Manual into 'dictionary'
        hbase.delete_metadata_tables()
        hbase.create_metadata_tables()
        load_mdrm_metadata(hbase, mdrm_path)
        logging.info(current_runtime(job_start_timestamp))
        logging.critical('refreshed MDRM definitions in `dictionary` from {path}, exiting...'.format(path=mdrm_path))
        sys.exit(0)

    if not ffiec_username:
        raise ValueError('provide a username for a FFIEC CDR account')

    if not ffiec_token:
        raise ValueError('provide an authentication token for FFIEC CDR account')

    if rssd_target is None:
        rssd_target = RSSD_WILDCARD
    rssd_target = int(rssd_target)

    ffiec = Extractor(ffiec_wsdl_url, ffiec_username, ffiec_token)
    ffiec.setup()

    # Start the actual ETL process
    for period in ffiec.reporting_periods():
        if period_is_filtered(period_target, period):
            continue

        reporters = ffiec.reporting_institutions(period)
        rssd_set = {int(reporter[ID_RSSD]) for reporter in reporters}

        # Load the actual call reports into `report`
        report_table = hbase.report_table.batch()
        for institution in reporters:
            rssd = int(institution[ID_RSSD])
            if rssd_is_filtered(rssd_target, rssd):
                continue

            if rssd not in rssd_set:
                logging.info('no report for reporter {rssd} at period {period}'.format(rssd=rssd, period=period))
                continue

            unicode_sdf_facsimile = Transformer.bytes_to_unicode(ffiec.call_report_facsimile(period, institution))
            facsimile = Transformer.sdf_to_dictreader(unicode_sdf_facsimile)

            for key in institution:
                row_key, column_key, value = Transformer.to_report__institution(institution[ID_RSSD], period, institution, key)
                report_table.put(row_key, {column_key: value})

                for item in facsimile:
                    mdrm = Transformer.normalize_mdrm(item[MDRM])
                    if mdrm is None:
                        logging.critical('MDRM is None, dropped a metric: {metric}'.format(metric=item))
                        continue

                    for key in item:
                        row_key, column_key, value = Transformer.to_report__call_report(rssd, period, item, key, mdrm)
                        report_table.put(row_key, {column_key: value})

            report_table.send()
            logging.info('loaded report::Institution into {rssd}-{period}'.format(rssd=rssd, period=period))
            logging.info('loaded report::CallReport into {rssd}-{period}'.format(rssd=rssd, period=period))
            logging.info(current_runtime(job_start_timestamp))

        # Load period=>institution lookup table `period`
        period_table = hbase.period_table.batch()
        for institution in reporters:
            rssd = int(institution[ID_RSSD])
            if rssd_is_filtered(rssd_target, rssd):
                continue

            row_key, column_key, value = Transformer.to_period__institution(period, institution[ID_RSSD], institution)
            period_table.put(row_key, {column_key: value})

        period_table.send()
        logging.info('loaded period=>institution lookup table for period {}'.format(period))
        logging.info(current_runtime(job_start_timestamp))

        # Load institution=>period lookup data into `institution`
        institution_table = hbase.institution_table.batch()
        for institution in reporters:
            rssd = int(institution[ID_RSSD])
            if rssd_is_filtered(rssd_target, rssd):
                logging.debug('filtering reporter rssd# {rssd}'.format(rssd=rssd))
                continue

            row_key, column_key, value = Transformer.to_institution__period(period, rssd, institution)
            institution_table.put(row_key, {column_key: value})

        institution_table.send()
        logging.info('loaded institution=>period lookup table for period {}'.format(period))
        logging.info(current_runtime(job_start_timestamp))

    total_runtime = (datetime.now() - job_start_timestamp)
    logging.warning('job completed in {time}'.format(time=total_runtime))
    sys.exit(0)

if __name__ == '__main__':
    main()
