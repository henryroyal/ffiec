import zeep
from zeep.wsse.username import UsernameToken


CALL_REPORT = 'Call'
ID_RSSD = 'ID_RSSD'
SDF = 'SDF'

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

    def _assert_client_initialized_or_fail(self):
        if not self.client:
            raise ValueError('soap client is uninitialized')

    def reporting_periods(self):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrieveReportingPeriods(CALL_REPORT)

    def reporting_institutions(self, period):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrievePanelOfReporters(CALL_REPORT, period)

    def call_report_facsimile(self, period, institution):
        self._assert_client_initialized_or_fail()
        return self.client.service.RetrieveFacsimile(CALL_REPORT, period, ID_RSSD, institution[ID_RSSD], SDF)

