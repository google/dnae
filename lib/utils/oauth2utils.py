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

"""Utility modules - oAuth2 utilities.

Utility methods to handle oAuth2 functionalities.
"""

import argparse
import os

import httplib2
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
from oauth2client.service_account import ServiceAccountCredentials
from utils import retry


_HTTP_TIMEOUT_SECONDS = 60


class OAuth2Authentication(object):
  """Provides different methods to authenticate through oAuth2.

  Attributes:
    api_scopes: the API scopes to authenticate for.
  """

  def __init__(self, api_scopes):
    self.__scopes = api_scopes

  @retry
  def authorize_from_keyfile(self, user_email, credentials_filename):
    """Authenticate using the Service Account and the delegate user e-mail.

    Args:
      user_email: user e-mail.
      credentials_filename: the credentials file name.

    Returns:
      The authorized http object.
    """
    filename = os.path.join(os.getcwd(), credentials_filename)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        filename, self.__scopes)
    delegated_credentials = credentials.create_delegated(user_email)
    http = delegated_credentials.authorize(http=httplib2.Http(
        timeout=_HTTP_TIMEOUT_SECONDS))
    delegated_credentials.refresh(http)
    return http

  @retry
  def authorize_from_clientsecrets(self, secrets_filename,
                                   credentials_filename):
    """Authenticate from a client secrets file and generate a credentials file.

    Args:
      secrets_filename: the JSON clients secrets file name.
      credentials_filename: the name for the generated credentials file.

    Returns:
      The authorized http object.
    """
    flow = client.flow_from_clientsecrets(
        os.path.join(os.getcwd(), secrets_filename), scope=self.__scopes)
    flags = self.__get_arguments([], __doc__)
    storage = oauthFile.Storage(credentials_filename)
    credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http(
        timeout=_HTTP_TIMEOUT_SECONDS))
    credentials.refresh(http)
    return http

  @retry
  def authorize_from_stored_credentials(self, credentials_filename):
    """Authenticate from the stored credentials file.

    Args:
      credentials_filename: the credentials file name.

    Returns:
      The authorized http object.
    """
    storage = oauthFile.Storage(credentials_filename)
    credentials = storage.get()
    http = credentials.authorize(http=httplib2.Http(
        timeout=_HTTP_TIMEOUT_SECONDS))
    return http

  def __get_arguments(self, argv, desc, parents=None):
    """Validates and parses command line arguments.

    Args:
      argv: list of strings, the command-line parameters of the application.
      desc: string, a description of the sample being executed.
      parents: list of argparse.ArgumentParser, additional command-line parsers.
    Returns:
        The parsed command-line arguments.
    """

    # Include the default oauth2client argparser
    parent_parsers = [tools.argparser]

    if parents:
      parent_parsers.extend(parents)

    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=parent_parsers)

    return parser.parse_args(argv[1:])
