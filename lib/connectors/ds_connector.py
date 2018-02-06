# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connector modules - Connector for DS.

This module provides access to DS API
"""
import copy
import csv
from StringIO import StringIO
import time

from api_handlers import APIRequest
from googleapiclient import discovery
from googleapiclient import http as httpMediaHandler
from oauth2utils import OAuth2Authentication
from utils import TextUtils

# DS Specific Exceptions


class DSAPITimeOut(Exception):
  pass


# DS Specific Classes


class DSReport(object):
  """Representation of a DS Report.
  """

  def __init__(self, body_template):
    self.__body = copy.deepcopy(body_template)

  def setscope(self, scope_name, scope_value):
    self.__body['reportScope'][scope_name] = scope_value

  def setdates(self, start, end):
    self.__body['timeRange']['startDate'] = start
    self.__body['timeRange']['endDate'] = end

  def addfilter(self, filter_name, filer_value):
    pass

  def addcolumn(self, column_name):
    self.__body['columns'].append({'columnName': column_name})

  def addsavedcolumn(self, column_name):
    self.__body['columns'].append({'savedColumnName': column_name})

  def getbody(self):
    return self.__body


class DSConnector(object):
  """DCM Connector class.

  This class provides methods to access DCM API for creating and downloading
  reports.
  """

  _scopes = ['https://www.googleapis.com/auth/doubleclicksearch']
  _apiver = 'v2'
  _DS_TIMEOUT = 3600 * 2  # 120 min

  def __init__(self, credential_file, user_email=None):
    oauth2 = OAuth2Authentication(self._scopes)
    if user_email:
      http = oauth2.authorize_from_keyfile(user_email, credential_file)
    else:
      http = oauth2.authorize_from_stored_credentials(credential_file)
    self.__http = http
    self.__api = discovery.build('doubleclicksearch', self._apiver, http=http)

  # Config methods

  def getdsresource(self):
    return self.__api

  # Reporting methods

  def createreport(self, report_obj):
    """Create a new report.

    Creates a new report using DS API. Creation of a report triggers its
    execution

    Args:
      report_obj: Report descriptor (see
        https://developers.google.com/doubleclick-search/v2/reference/reports#resource-representations)
    Returns:
      ID of the newly created report
    """
    assert isinstance(report_obj, DSReport)
    request = self.__api.reports().request(body=report_obj.getbody())
    result = APIRequest(request).execute()
    report_id = result['id']
    return report_id

  def getreportdata(self, report_id, sanitize_rows=True):
    """Get report data.

    Download report data, once executed. This methods blocks until report
    has finished executing.

    Args:
      report_id: ID of the report to be downloaded
      sanitize_rows: Whether to remove commas and quotes from the rows in the
        report or not
    Returns:
      List with all the rows in the report
    """
    dataio = self.__getreportdataraw(report_id)
    reader = csv.reader(dataio)
    data = list()
    for row in reader:
      temp_row = row
      if sanitize_rows:
        temp_row = TextUtils.removecommas(temp_row)
        temp_row = TextUtils.removequotes(temp_row)
      data.append(temp_row)
    return data

  def __getreportdataraw(self, report_id):
    request = self.__api.reports().get(reportId=report_id)
    result = APIRequest(request).execute()

    et = 0
    retry_attempts = 0
    while True:
      if result['isReportReady']:
        request = self.__api.reports().getFile(
            reportId=report_id, reportFragment=0)
        data = StringIO()
        downloader = httpMediaHandler.MediaIoBaseDownload(
            data, request, chunksize=2 ** 20 * 20)  # 20Mb chunks
        done = False
        while done is False:
          unused_status, done = downloader.next_chunk()
        data.seek(0)
        return data
      seconds = 2 ** retry_attempts
      retry_attempts += 1
      retry_attempts %= 11  # reset to 0 if >10
      time.sleep(seconds)
      et += seconds
      if et >= DSConnector._DS_TIMEOUT:
        raise DSAPITimeOut('DS API Request Timeout (files.get())')

      request = self.__api.reports().get(reportId=report_id)
      result = APIRequest(request).execute()
