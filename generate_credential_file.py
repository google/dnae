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

"""DNA - Generates API credentials file.

A script to generate standalone credential files to use the necessary API.
"""

import sys
sys.path.append('lib/utils')
sys.path.append('lib/core')
import oauth2utils
from dna_general_settings import CREDENTIAL_FILE

CLIENT_SECRETS_FILE = 'client_secret.json'

API_SCOPES = [
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/cloud-tasks',
  'https://www.googleapis.com/auth/spreadsheets.readonly',
]

API_SCOPES_MINIMUM = [
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/cloud-tasks',
]


def from_setup(needs_dbm, needs_dcm, needs_ds, needs_sheets):
  """Function called from the setup script, with additional API scopes.

  Args:
    needs_dbm: whether the DBM API scope is also needed (Y/N).
    needs_dcm: whether the DCM API scope is also needed (Y/N).
    needs_ds: whether the DS API scope is also needed (Y/N).
    needs_sheets: whether the Sheets API scope is also needed (Y/N).
  """
  scopes = API_SCOPES_MINIMUM
  if needs_dbm == 'Y':
    scopes.append('https://www.googleapis.com/auth/doubleclickbidmanager')
  if needs_dcm == 'Y':
    scopes.append('https://www.googleapis.com/auth/dfareporting')
    scopes.append('https://www.googleapis.com/auth/dfatrafficking')
  if needs_ds == 'Y':
    scopes.append('https://www.googleapis.com/auth/doubleclicksearch')
  if needs_sheets == 'Y':
    scopes.append('https://www.googleapis.com/auth/spreadsheets.readonly')

  main(scopes)


def main(scopes):
  """Generates standalone credentials file with the provided scopes."""
  oauth2 = oauth2utils.OAuth2Authentication(scopes)
  oauth2.authorize_from_clientsecrets(CLIENT_SECRETS_FILE, CREDENTIAL_FILE)


if __name__ == '__main__':
  main(API_SCOPES)
