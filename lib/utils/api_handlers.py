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

"""Utility modules - API related utilities.

Utility methods to handle API requests.
"""

import json
import logging
import time


class RESTAPICallError(Exception):
  pass


class APIRequest(object):
  """Provides methods to try (and re-try) API requests.

  Execute an API request and try again if it fails (with a _MAX_RETRIES number
  of attempts.

  Attributes:
    request: the actual API request.
  """

  _MAX_RETRIES = 5

  def __init__(self, request):
    self.request = request
    self._retry_attempts = 0

  def execute(self):
    """Executes the API request.

    If some error occurs during the execution, new attempts are launched after
    an increasingly longer interval, and until a max number of attempts is
    reached.

    Returns:
      The API reques result.
    """
    while True:
      try:
        return self.request.execute()
      # pylint: disable=broad-except
      except Exception as e:
        self._retryrequest(e)
      # pylint: enable=broad-except

  def _retryrequest(self, error):
    self._retry_attempts += 1
    if self._retry_attempts <= self._MAX_RETRIES:
      seconds = 2 ** self._retry_attempts
      logging.warning('[API Request] Encountered an error - %s -, '
                      'retrying in %d seconds...', str(error), seconds)
      time.sleep(seconds)
    else:
      raise error


class RESTAPIRequest(object):
  """Provides methods to try (and re-try) Rest API requests.

  Execute a REST API request and try again if it fails (with a _MAX_RETRIES
  number of attempts.
  """

  _MAX_RETRIES = 5

  def __init__(self):
    self._retry_attempts = 0

  def execute(self, http, url, method, body=None):
    """Executes the Rest API request.

    Args:
      http: the http object.
      url: REST API url.
      method: API method (get/post).
      body: optional body object for the request.

    Returns:
      The REST API request result.
    """
    while True:
      try:
        if body:
          body_length = len(body)
          headers = {
              'Content-type': 'application/json',
              'Content-length': '%s' % body_length
          }
          response = http.request(
              url, method=method, body=body, headers=headers)
        else:
          if method == 'POST':
            headers = {'Content-length': '0'}
            response = http.request(url, method=method, headers=headers)
          else:
            response = http.request(url, method=method)

        if response[0].status >= 400:
          try:
            result = json.loads(response[1])
            raise RESTAPICallError(result.get('error').get('message'))
          except:
            raise RESTAPICallError(response[1])

        if method != 'DELETE':
          result = json.loads(response[1])
        else:
          result = 0

        return result

      # pylint: disable=broad-except
      except Exception as e:
        self._wait_or_raise_error(e)
      # pylint: enable=broad-except

  def _wait_or_raise_error(self, error):
    self._retry_attempts += 1
    if self._retry_attempts <= self._MAX_RETRIES:
      seconds = 2 ** self._retry_attempts
      logging.warning('[REST API Request] Encountered an error - %s -, '
                      'retrying in %d seconds...', str(error), seconds)
      time.sleep(seconds)
    else:
      raise error
