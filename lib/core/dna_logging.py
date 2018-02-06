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

"""DNA - Logging setup.

Logging configuration.
"""
import logging
from google.cloud import logging as cloud_logging


def configure_logging():
  """Configure DNA to use cloud logging.

  Invoke once at application startup, before any log calls.
  """
  client = cloud_logging.Client()
  client.setup_logging(log_level=logging.DEBUG)
