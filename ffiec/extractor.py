import logging
import zeep
import zeep.exceptions
from zeep.wsse.username import UsernameToken


class Extractor:

    CALL_REPORT = 'Call'
    ID_RSSD = 'ID_RSSD'
    SDF = 'SDF'

    def __init__(self, wsdl_url, username, token):
        self.wsdl_url = wsdl_url
        self.username = username
        self.token = token
        self.client = None

    def setup(self):
        self.client = zeep.Client(wsdl=self.wsdl_url, wsse=UsernameToken(self.username, self.token))

        if not self.client.service.TestUserAccess():
            raise ValueError('API authentication failed, check your username and token - did you rotate your token?')

    def _assert_client_initialized_or_fail(self):
        if not self.client:
            raise ValueError('soap client is uninitialized')

    def reporting_periods(self):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrieveReportingPeriods(self.CALL_REPORT)

    def reporting_institutions(self, period):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrievePanelOfReporters(self.CALL_REPORT, period)

    def call_report_facsimile(self, period, institution):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrieveFacsimile(self.CALL_REPORT, period, self.ID_RSSD,
                                                     institution[self.ID_RSSD], self.SDF)
