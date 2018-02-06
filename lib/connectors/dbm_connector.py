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

"""Connector modules - Connector for DBM.

Connector for retrieving data using DBM Reporting API
"""
import copy
import csv
import datetime
import io
import json
import logging
from StringIO import StringIO
import time
import urllib2

from api_handlers import APIRequest
from googleapiclient import discovery
from googleapiclient import http as httpMediaHandler
from oauth2utils import OAuth2Authentication
from utils import DateUtils
from utils import TextUtils

# DBM Specific Exceptions


class DBMAPITimeOut(Exception):
  pass


class DBMAPIQueryFailed(Exception):
  pass


# DBM Specific Classes


class DBMQuery(object):
  """Class for building DBM API requests.
  """

  def __init__(self, body_template):
    self.__body = copy.deepcopy(body_template)

  def setdates(self, data_range, start_date=None, end_date=None):
    if data_range:
      self.__body['metadata']['dataRange'] = data_range
    if start_date:
      assert isinstance(start_date, datetime.datetime)
      assert isinstance(end_date, datetime.datetime)
      self.__body['reportDataStartTimeMs'] = time.mktime(
          start_date.timetuple()) * 1000
      self.__body['reportDataEndTimeMs'] = time.mktime(
          end_date.timetuple()) * 1000

  def settitle(self, title):
    self.__body['metadata']['title'] = title

  def addfilter(self, filter_name, filer_value):
    self.__body['params']['filters'].append({
        'type': filter_name,
        'value': filer_value
    })

  def getbody(self):
    return self.__body


class DBMConnector(object):
  """DBM Connector class.

  Manages communication with DBM API
  """
  _scopes = [
      'https://www.googleapis.com/auth/devstorage.full_control',
      'https://www.googleapis.com/auth/doubleclickbidmanager'
  ]

  _apiver = 'v1'
  _gcs_apiver = 'v1'

  _DBM_TIMEOUT = 3600 * 2  # 60 min

  def __init__(self, credential_file, user_email=None):
    csv.field_size_limit(100 * 2 ** 20)  # 100Mb
    oauth2 = OAuth2Authentication(self._scopes)
    if user_email:
      http = oauth2.authorize_from_keyfile(user_email, credential_file)
    else:
      http = oauth2.authorize_from_stored_credentials(credential_file)
    self.__api = discovery.build(
        'doubleclickbidmanager', self._apiver, http=http)
    self.__gcs = discovery.build('storage', self._gcs_apiver, http=http)

  def runquery(self, query_id, data_range=None, start_date=None, end_date=None):
    """Run a specified query.

    This method runs the specified query (report). You can optionally
    specify date ranges.

    Args:
      query_id: ID of the query to run
      data_range: Range for report as expected in metadata.dataRange field
        (see https://developers.google.com/bid-manager/v1/queries#resource)
      start_date: Only applicable if data_range is CUSTOM_DATES
      end_date: Only applicable if data_range is CUSTOM_DATES
    """
    body = {}
    if data_range:
      body['dataRange'] = data_range
      if start_date:
        assert isinstance(start_date, datetime.datetime)
        assert isinstance(end_date, datetime.datetime)
        body['reportDataStartTimeMs'] = time.mktime(
            start_date.timetuple()) * 1000
        body['reportDataEndTimeMs'] = time.mktime(end_date.timetuple()) * 1000
    request = self.__api.queries().runquery(queryId=query_id, body=body)
    APIRequest(request).execute()

  def createquery(self, body):
    """Create new query.

    This method creates a new query (report).

    Args:
      body: JSON object describing report. See 'Queries resource' on DBM API
        docs (https://developers.google.com/bid-manager/v1/queries#resource)
    Returns:
      ID of the newly created query
    """
    request = self.__api.queries().createquery(body=body)
    result = APIRequest(request).execute()
    return result.get('queryId')

  def deletequery(self, query_id):
    """Delete query.

    Remove the query with the specified ID from the system.

    Args:
      query_id: ID of the query to be deleted
    """
    request = self.__api.queries().deletequery(queryId=query_id)
    APIRequest(request).execute()

  def getquerydata(self, query_id, sanitize_rows=True, remove_last_row=True):
    """Get query data.

    Download query (report) actual data.

    Args:
      query_id: ID of the report to download
      sanitize_rows: Remove commas, quotes and new lines from report lines
      remove_last_row: Remove last row (typically totals will be in this row)
    Returns:
      List with lines in the report
    """
    dataio = self.__getquerydataraw(query_id)
    data = list()
    if dataio.len > 0:
      reader = csv.reader(dataio)
      for row in reader:
        if not row:
          break
        temp_row = row
        if sanitize_rows:
          temp_row = TextUtils.removecommas(temp_row)
          temp_row = TextUtils.removequotes(temp_row)
          temp_row = TextUtils.removenewlines(temp_row)
        data.append(temp_row)
    logging.debug('Report data retreived. Number of lines: %s', len(data))
    # We remove the last row (with totals) only if there's more than one entry
    # (or totals will not be there)
    if (remove_last_row) and (len(data) > 2):
      return data[:-1]
    else:
      return data

  def loadentities(self, partner_id, entity_type):
    """Load DBM entities from Entity Read files.

    Load entities for a given DBM partner from DBM's Entity Read files.

    Args:
      partner_id: ID of the partner
      entity_type: Type of entity to load
    Returns:
      JSON object with representation of the entities loaded
    """
    bucket_name = 'gdbm-{partner_id}'.format(partner_id=partner_id)

    yesterday = datetime.datetime.fromordinal(DateUtils.today.toordinal() - 1)
    today = datetime.datetime.fromordinal(DateUtils.today.toordinal())

    yesterday_str = yesterday.strftime('%Y%m%d')

    today_str = today.strftime('%Y%m%d')

    yesterday_entity_file_name = 'entity/{date}.0.{type}.json'.format(
        date=yesterday_str, type=entity_type)
    today_entity_file_name = 'entity/{date}.0.{type}.json'.format(
        date=today_str, type=entity_type)

    try:
      data = self.__loadobjectfromgcs(bucket_name, today_entity_file_name)
    # pylint: disable=broad-except
    except Exception as e:
      logging.warn('Could not retrieve object for today. Trying with '
                   'yesterday: %s', e)
      data = self.__loadobjectfromgcs(bucket_name, yesterday_entity_file_name)
    # pylint: enable=broad-except

    return data

  def sdfdownloadio(self, advertiser_id, sanitize_rows=True):
    """Download Insertion Orders in SDF format.

    Args:
      advertiser_id: DBM advertiser ID
      sanitize_rows: Whether to remove commas, quotes and new lines from each
        row
    Returns:
      List with rows, one per Insertion Order
    """
    body = {
        'fileTypes': ['INSERTION_ORDER'],
        'filterType': 'ADVERTISER_ID',
        'filterIds': []
    }
    body['filterIds'].append(advertiser_id)
    request = self.__api.sdf().download(body=body)
    sdfdata = APIRequest(request).execute()
    data = list()
    dataio = TextUtils.toascii(sdfdata['insertionOrders'])
    if dataio:
      reader = csv.reader(StringIO(dataio))
      for row in reader:
        if not row:
          break
        temp_row = row
        if sanitize_rows:
          temp_row = TextUtils.removecommas(temp_row)
          temp_row = TextUtils.removequotes(temp_row)
          temp_row = TextUtils.removenewlines(temp_row)
        data.append(temp_row)
    return data

  def sdfdownloadli(self, advertiser_id, sanitize_rows=True):
    """Download Line Items in SDF format.

    Args:
      advertiser_id: DBM advertiser ID
      sanitize_rows: Whether to remove commas, quotes and new lines from each
        row
    Returns:
      List with rows, one per Line Item
    """
    body = {
        'fileTypes': ['LINE_ITEM'],
        'filterType': 'ADVERTISER_ID',
        'filterIds': []
    }
    body['filterIds'].append(advertiser_id)
    request = self.__api.sdf().download(body=body)
    sdfdata = APIRequest(request).execute()
    data = list()
    dataio = TextUtils.toascii(sdfdata['lineItems'])
    if dataio:
      reader = csv.reader(StringIO(dataio))
      for row in reader:
        if not row:
          break
        temp_row = row
        if sanitize_rows:
          temp_row = TextUtils.removecommas(temp_row)
          temp_row = TextUtils.removequotes(temp_row)
          temp_row = TextUtils.removenewlines(temp_row)
        data.append(temp_row)
    return data

  def sdfdownloadadgroup(self, advertiser_id, sanitize_rows=True):
    """Download Ad Groups in SDF format.

    Args:
      advertiser_id: DBM advertiser ID
      sanitize_rows: Whether to remove commas, quotes and new lines from each
        row
    Returns:
      List with rows, one per Ad Group
    """
    body = {
        'fileTypes': ['AD_GROUP'],
        'filterType': 'ADVERTISER_ID',
        'filterIds': []
    }
    body['filterIds'].append(advertiser_id)
    request = self.__api.sdf().download(body=body)
    sdfdata = APIRequest(request).execute()
    data = list()
    dataio = TextUtils.toascii(sdfdata['adGroups'])
    if dataio:
      reader = csv.reader(StringIO(dataio))

      for row in reader:
        if not row:
          break
        temp_row = row
        if sanitize_rows:
          temp_row = TextUtils.removecommas(temp_row)
          temp_row = TextUtils.removequotes(temp_row)
          temp_row = TextUtils.removenewlines(temp_row)
        data.append(temp_row)
    return data

  def __getquerydataraw(self, query_id):
    """Get query data, wait if necessary.

    Retrieve query data. Waits for the data to be ready and then downloads
    data directly.

    Args:
      query_id: ID of the query
    Returns:
      File-like object with the raw report data
    Raises:
      DBMAPITimeOut: If report is not ready before DBMConnector._DBM_TIMEOUT
        seconds
      DBMAPIQueryFailed: If report is ready but doesn't contain any files
    """
    et = 0
    retry_attempts = 0
    while True:
      request = self.__api.queries().getquery(queryId=query_id)
      result = APIRequest(request).execute()
      if 'googleCloudStoragePathForLatestReport' in result['metadata']:
        if len(result['metadata']['googleCloudStoragePathForLatestReport']) > 1:
          break
      seconds = 2 ** retry_attempts
      retry_attempts += 1
      retry_attempts %= 9  # reset to 0 if >8
      time.sleep(seconds)
      et += seconds
      if et > DBMConnector._DBM_TIMEOUT:
        raise DBMAPITimeOut(
            'DBM API Request Timeout (getquery) - Query ID: %s' % query_id)

    if 'googleCloudStoragePathForLatestReport' in result['metadata']:
      report_url = result['metadata']['googleCloudStoragePathForLatestReport']
      data = urllib2.urlopen(report_url).read()
      return StringIO(data)
    else:
      raise DBMAPIQueryFailed('DBM Query Failed - Query ID: %s' % query_id)

  def __loadobjectfromgcs(self, bucket, gcs_object):
    """Load object from GCS.

    Load object from Google Cloud Storage.

    Args:
      bucket: Name of the bucket
      gcs_object: GCS object to be retrieved
    Returns:
      JSON for the DBM object as read from GCS
    """
    req = self.__gcs.objects().get_media(bucket=bucket, object=gcs_object)

    fh = io.BytesIO()
    downloader = httpMediaHandler.MediaIoBaseDownload(
        fh, req, chunksize=2 ** 20 * 20)
    done = False
    while not done:
      unused_status, done = downloader.next_chunk()

    return json.loads(fh.getvalue())
