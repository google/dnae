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

"""Connector modules - Connector for Google Sheets.

This module provides access to Google Sheets
"""
from api_handlers import RESTAPIRequest
from oauth2utils import OAuth2Authentication

# Google Sheets Specific Exceptions


class GSHEETAPITimeOut(Exception):
  pass


# Google Sheets Specific Classes


class GSheetConnector(object):
  """Google Sheets Connector class.

  This class provides methods to access Google Sheets data.
  """
  _scopes = [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/drive.readonly',
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/spreadsheets.readonly'
  ]
  _apiver = 'v4'

  def __init__(self, credential_file, user_email=None):
    oauth2 = OAuth2Authentication(self._scopes)
    if user_email:
      http = oauth2.authorize_from_keyfile(user_email, credential_file)
    else:
      http = oauth2.authorize_from_stored_credentials(credential_file)
    self.__http = http

  def getvalues(self, spreadsheet_id, data_range):
    url = ('https://sheets.googleapis.com/{ver}/'
           'spreadsheets/{spreadsheetId}/values/{range}').format(
               ver=self._apiver, spreadsheetId=spreadsheet_id, range=data_range)
    result = RESTAPIRequest().execute(self.__http, url, 'GET')

    return result
