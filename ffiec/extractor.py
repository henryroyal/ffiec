import logging

import zeep
from zeep.wsse.username import UsernameToken

class Extractor:
    def __init__(self, wsdl_url, username, token):
        self.wsdl_url = wsdl_url
        self.username = username
        self.token = token
        self.client = None

    def setup(self):
        self.client = zeep.Client(wsdl=self.wsdl_url, wsse=UsernameToken(self.username, self.token))

        if not self.client.service.TestUserAccess():
            raise ValueError('API authentication failed, check your username and token - did you rotate your token?')

        logging.debug('API authentication success')

    def _assert_client_initialized_or_fail(self):
        if not self.client:
            raise ValueError('soap client is uninitialized')

    def reporting_periods_by_series(self, series):
        return self.client.service.RetrieveReportingPeriods(series)
