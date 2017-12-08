import happybase
import logging

#TODO tune these definitions
REPORT_TABLE = 'report'
REPORT_TABLE_DEFINITION = {'CallReport': dict(), 'Institution': dict()}

PERIOD_TABLE = 'period'
PERIOD_TABLE_DEFINITION = {'ReportPeriod': dict()}

INSTITUTION_TABLE = 'institution'
INSTITUTION_TABLE_DEFINITION = {'Institution': dict()}

DATA_DICTIONARY = 'dictionary'
DATA_DICTIONARY_DEFINITION = {'Metadata': dict()}


class Hbase:
    def __init__(self, hbase_master):
        self.hbase_master = hbase_master
        self.connection = None

    def _assert_connected(self):
        if not self.connection:
            raise ValueError('hbase connection not initialized')

    def connect(self):
        self.connection = happybase.Connection(self.hbase_master)

    @property
    def report_table(self):
        return self.connection.table(REPORT_TABLE)

    @property
    def period_table(self):
        return self.connection.table(PERIOD_TABLE)

    @property
    def institution_table(self):
        return self.connection.table(INSTITUTION_TABLE)

    @property
    def data_dictionary_table(self):
        return self.connection.table(DATA_DICTIONARY)

    def _disable_table(self, table):
        try:
            self.connection.disable_table(table)
            logging.debug('disabled table {}'.format(table))
        except Exception as err:
            logging.error(err)

    def _delete_table(self, table):
        try:
            self.connection.delete_table(table)
            logging.debug('deleted table {}'.format(table))
        except Exception as err:  # FIXME - built thrift objects and make this correct
            logging.error(err)

    def _create_table(self, table, definition):
        try:
            logging.debug('created table {}'.format(table))
            self.connection.create_table(table, definition)
        except Exception as err:
            logging.error(err)
            raise err

    def truncate_data_dictionary(self):
        self._disable_table(DATA_DICTIONARY)
        self._delete_table(DATA_DICTIONARY)
        self._create_table(DATA_DICTIONARY, DATA_DICTIONARY_DEFINITION)

    def delete_all_tables(self):
        for table in (REPORT_TABLE, PERIOD_TABLE, INSTITUTION_TABLE, DATA_DICTIONARY):
            self._disable_table(table)
            self._delete_table(table)
        logging.warning('deleted hbase tables')

    def create_all_tables(self):
        for table, definition in ((REPORT_TABLE, REPORT_TABLE_DEFINITION),
                                  (PERIOD_TABLE, PERIOD_TABLE_DEFINITION),
                                  (INSTITUTION_TABLE, INSTITUTION_TABLE_DEFINITION),
                                  (DATA_DICTIONARY, DATA_DICTIONARY_DEFINITION)):

            self._create_table(table, definition)
        logging.warning('created hbase tables')
