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

"""Connector modules - Connector for DCM.

This module provides access to DCM Reporting API
"""
import copy
import csv
from StringIO import StringIO
import time
import urllib2

from api_handlers import APIRequest
from googleapiclient import discovery
from googleapiclient import http as httpMediaHandler
from oauth2utils import OAuth2Authentication
from utils import TextUtils

# DCM Specific Constants

DCM_DATE_RANGES = [
    'LAST_24_MONTHS', 'LAST_30_DAYS', 'LAST_365_DAYS', 'LAST_7_DAYS',
    'LAST_90_DAYS', 'MONTH_TO_DATE', 'PREVIOUS_MONTH', 'PREVIOUS_QUARTER',
    'PREVIOUS_WEEK', 'PREVIOUS_YEAR', 'QUARTER_TO_DATE', 'TODAY',
    'WEEK_TO_DATE', 'YEAR_TO_DATE', 'YESTERDAY'
]

# DCM Specific Exceptions


class DCMAPITimeOut(Exception):
  pass


class DCMReportInvalidRange(Exception):
  pass


# DCM Specific Classes


class DCMReport(object):
  """Representation of a DCM Report.
  """

  def __init__(self, body_template):
    self.__body = copy.deepcopy(body_template)
    self.__body[self.__getcriteriakey()]['dimensionFilters'] = list()

  def setname(self, name):
    self.__body['name'] = name
    self.__body['fileName'] = name

  def setdates(self, date_range, start=None, end=None):
    if date_range == 'CUSTOM':
      assert start is not None
      assert end is not None
      self.__body[self.__getcriteriakey()]['dateRange'] = {
          'startDate': start,
          'endDate': end
      }
    elif date_range in DCM_DATE_RANGES:
      self.__body[self.__getcriteriakey()]['dateRange'] = {
          'relativeDateRange': date_range
      }
    else:
      raise DCMReportInvalidRange(date_range)

  def addfilter(self, filter_name, filer_value):
    self.__body[self.__getcriteriakey()]['dimensionFilters'].append({
        'dimensionName': filter_name,
        'id': filer_value
    })

  def adddimension(self, dimension_name):
    self.__body[self.__getcriteriakey()]['dimensions'].append({
        'name': dimension_name
    })

  def getbody(self):
    return self.__body

  def __getcriteriakey(self):
    criteria = 'criteria'
    report_type = self.__body['type']
    if report_type == 'REACH':
      criteria = 'reachCriteria'
    return criteria


class DCMConnector(object):
  """DCM Connector class.

  This class provides methods to access DCM API for creating and downloading
  reports.
  """

  # Private properties

  _scopes = [
      'https://www.googleapis.com/auth/dfareporting',
      'https://www.googleapis.com/auth/dfatrafficking'
  ]

  _DCM_TIMEOUT = 3600 * 2  # 120 min

  # Constructor
  def __init__(self, credential_file, user_email, profile_id, api_version):
    oauth2 = OAuth2Authentication(self._scopes)
    if user_email:
      http = oauth2.authorize_from_keyfile(user_email, credential_file)
    else:
      http = oauth2.authorize_from_stored_credentials(credential_file)
    self.__http = http
    self.__ver = api_version
    self.__api = discovery.build('dfareporting', self.__ver, http=http)
    self.__profile_id = profile_id

  # Config methods

  def getdcmresource(self):
    return self.__api

  # Trafficking methods

  def getadvertisers(self):
    """Get list of advertisers.

    Get advertisers from DCM API.

    Returns:
      List of advertisers resource (see
        https://developers.google.com/doubleclick-advertisers/v2.8/advertisers#resource)
    """
    request = self.__api.advertisers().list(profileId=self.__profile_id)
    result = APIRequest(request).execute()
    return result['advertisers']

  def getads(self, campaign_id):
    """Get list of ads.

    Get list of ads from DCM API.

    Args:
      campaign_id: Campaign ID. Only ads under this campaign will be
        returned
    Returns:
      List of ads resource (see
        https://developers.google.com/doubleclick-advertisers/v2.8/ads#resource)
    """
    request = self.__api.ads().list(
        profileId=self.__profile_id, campaingId=campaign_id)
    result = APIRequest(request).execute()
    return result['ads']

  def getcreatives(self, campaign_id):
    """Get list of creatives.

    Get list of creatives from DCM API.

    Args:
      campaign_id: Campaign ID. Only creatives under this campaign will be
        returned
    Returns:
      List of creatives resource (see
        https://developers.google.com/doubleclick-advertisers/v2.8/creatives)
    """
    request = self.__api.ads().list(
        profileId=self.__profile_id, campaingId=campaign_id)
    result = APIRequest(request).execute()
    return result['creatives']

  # Reporting methods

  def getreports(self, scope='MINE'):
    """Get list of reports.

    Get list of reports from DCM API.

    Args:
      scope: Scope for filtering the reports
    Returns:
      List of reports resource (see
        https://developers.google.com/doubleclick-advertisers/v2.8/reports)
    """
    token = None
    report_list = list()
    while True:
      result = self._get_reports_page_result(scope, token)
      report_list += result['items']
      token = urllib2.quote(result['nextPageToken'], safe='')
      if not token:
        return report_list

  def createreport(self, report_obj):
    """Creates a new report.

    Creates a new DCM report.

    Args:
      report_obj: Report resource (see
        https://developers.google.com/doubleclick-advertisers/v2.8/reports#resource)
    Returns:
      ID of the newly created report
    """
    assert isinstance(report_obj, DCMReport)
    request = self.__api.reports().insert(
        profileId=self.__profile_id, body=report_obj.getbody())
    result = APIRequest(request).execute()
    return result['id']

  def runreport(self, report_id):
    """Run a report.

    Execute the report with the provided ID.

    Args:
      report_id: Report ID
    Returns:
      File ID for this report execution
    """
    request = self.__api.reports().run(
        profileId=self.__profile_id, reportId=report_id)
    result = APIRequest(request).execute()
    return result['id']

  def deletereport(self, report_id):
    """Delete a report.

    Delete a report with the given ID.

    Args:
      report_id: Report ID.
    """
    request = self.__api.reports().delete(
        profileId=self.__profile_id, reportId=report_id)
    APIRequest(request).execute()

  def getreportdata(self,
                    report_id,
                    file_id,
                    sanitize_rows=True,
                    chunk_size=2 ** 20 * 30):
    """Get actual report data.

    Retrieve a report data. You may invoke this method after the
    report has been executed.

    Args:
      report_id: Report ID
      file_id: ID of the file to be retrieved (each report might be executed
        several times, each will yield a new file ID)
      sanitize_rows: Whether to remove commas, quotes and newlines from the
        report's rows or not
      chunk_size: Download chunk size
    Returns:
      List with all the rows in the report except for the last one (which
      corresponds to totals)
    """
    dataio = self.__getreportdataraw(report_id, file_id, chunk_size)
    reader = csv.reader(dataio)
    data = list()
    for row in reader:
      if row and row[0] == 'Report Fields':
        break
    for row in reader:
      temp_row = row
      if sanitize_rows:
        temp_row = TextUtils.removecommas(temp_row)
        temp_row = TextUtils.removequotes(temp_row)
        temp_row = TextUtils.removenewlines(temp_row)
      data.append(temp_row)
    return data[0:-1]

  # Private methods

  def _get_reports_page_result(self, scope, page_token):
    request = self.__api.reports().list(
        profileId=self.__profile_id, scope=scope, pageToken=page_token)
    return APIRequest(request).execute()

  def __getreportdataraw(self, report_id, file_id, chunk_size):
    request = self.__api.files().get(reportId=report_id, fileId=file_id)
    result = APIRequest(request).execute()

    et = 0
    retry_attempts = 0
    while True:
      if result['status'] == 'REPORT_AVAILABLE':
        request = self.__api.files().get_media(
            reportId=report_id, fileId=file_id)
        data = StringIO()
        downloader = httpMediaHandler.MediaIoBaseDownload(
            data, request, chunksize=chunk_size)
        done = False
        while done is False:
          unused_status, done = downloader.next_chunk(num_retries=4)
        data.seek(0)
        return data
      seconds = 2 ** retry_attempts
      retry_attempts += 1
      retry_attempts %= 11  # reset to 0 if >10
      time.sleep(seconds)
      et += seconds
      if et >= DCMConnector._DCM_TIMEOUT:
        raise DCMAPITimeOut('DCM API Request Timeout (files.get())')

      request = self.__api.files().get(reportId=report_id, fileId=file_id)
      result = APIRequest(request).execute()
