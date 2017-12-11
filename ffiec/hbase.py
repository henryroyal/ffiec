import happybase
import logging


REPORT_TABLE = 'report'
REPORT_TABLE_DEFINITION = {'R': {'max_versions': 1,
                                 'in_memory': True,
                                 'bloom_filter_type': 'ROWCOL'}}

PERIOD_TABLE = 'period'
PERIOD_TABLE_DEFINITION = {'I': {'max_versions': 1,
                                 'in_memory': True,
                                 'bloom_filter_type': 'ROW'}}

INSTITUTION_TABLE = 'institution'
INSTITUTION_TABLE_DEFINITION = {'P': {'max_versions': 1,
                                      'in_memory': True,
                                      'bloom_filter_type': 'ROW'}}

DATA_DICTIONARY = 'dictionary'
DATA_DICTIONARY_DEFINITION = {'M': {'max_versions': 1,
                                    'in_memory': True,
                                    'bloom_filter_type': 'ROW'}}


class Hbase:
    def __init__(self, thrift_gateway, thrift_port):
        self.thrift_gateway = thrift_gateway
        self.thrift_port = thrift_port
        self.connection = None

    def connect(self):
        self.connection = happybase.Connection(host=self.thrift_gateway, port=self.thrift_port)

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
            self.connection.create_table(table, definition)
            logging.debug('created table {}'.format(table))
        except Exception as err:
            logging.error(err)
            raise err

    def delete_dictionary_table(self):
        self._disable_table(DATA_DICTIONARY)
        self._delete_table(DATA_DICTIONARY)

    def create_dictionary_table(self):
        self._create_table(DATA_DICTIONARY, DATA_DICTIONARY_DEFINITION)

    def delete_report_table(self):
        self._disable_table(REPORT_TABLE)
        self._delete_table(REPORT_TABLE)

    def create_report_table(self):
        self._create_table(REPORT_TABLE, REPORT_TABLE_DEFINITION)

    def delete_lookup_tables(self):
        for table in (PERIOD_TABLE, INSTITUTION_TABLE):
            self._disable_table(table)
            self._delete_table(table)

    def create_lookup_tables(self):
        for table, definition in ((PERIOD_TABLE, PERIOD_TABLE_DEFINITION),
                                  (INSTITUTION_TABLE, INSTITUTION_TABLE_DEFINITION)):
            self._create_table(table, definition)
