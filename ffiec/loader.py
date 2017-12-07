import happybase
import logging

#TODO tune these definitions
#report.CallReport:
#report.ReportPeriod:
REPORT_TABLE = 'report'
REPORT_TABLE_DEFINITION = {'CallReport': dict(), 'Institution': dict()}

PERIOD_TABLE = 'period'
PERIOD_TABLE_DEFINITION = {'ReportPeriod': dict()}

INSTITUTION_TABLE = 'institution'
INSTITUTION_TABLE_DEFINITION = {'Institution': dict()}

MDRM_TABLE = 'mdrm'
MDRM_TABLE_DEFINITION = {'MDRM': dict()}


class Loader:
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
    def mdrm_table(self):
        return self.connection.table(MDRM_TABLE)

    def delete_tables(self):
        for table in (REPORT_TABLE, PERIOD_TABLE, INSTITUTION_TABLE, MDRM_TABLE):
            try:
                self.connection.disable_table(table)
                logging.debug('disabled table {}'.format(table))
            except Exception as err:
                logging.error(err)

            try:
                self.connection.delete_table(table)
                logging.debug('deleted table {}'.format(table))
            except Exception as err: #FIXME - built thrift objects and make this correct
                logging.error(err)

        logging.warning('deleted hbase tables')

    def create_tables(self):
        for table, definition in ((REPORT_TABLE, REPORT_TABLE_DEFINITION),
                                  (PERIOD_TABLE, PERIOD_TABLE_DEFINITION),
                                  (INSTITUTION_TABLE, INSTITUTION_TABLE_DEFINITION),
                                  (MDRM_TABLE, MDRM_TABLE_DEFINITION)):
            try:
                logging.debug('created table {}'.format(table))
                self.connection.create_table(table, definition)
            except Exception as err:
                logging.error(err)
                continue

        logging.warning('created hbase tables')
