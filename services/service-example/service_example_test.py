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

"""DNA - Service example - Module to launch a local test."""
import os
import sys

# This is a workaround to import the relevant libraries for local testing
# purposes.
# The import will be handled through proper "vendor.add" in appengine_config.py
# for the AppEngine deployed version.
_BASEPATH = os.path.abspath(__file__).split(os.path.sep)[:-3]
for p in ('lib/connectors', 'lib/core', 'lib/utils'):
  sys.path.append(os.path.sep.join(_BASEPATH + [p]))

from dcm_connector import DCMConnector
from dna_general_settings import DCM_API_VER
from dna_project_settings import DCM_PROFILE_ID
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector
import service_example_run
from service_example_settings import DATA_SCHEMA_STANDARD
from service_example_settings import DCM_REPORT_DATE_RANGE
from service_example_settings import DCM_REPORT_NAME
from service_example_settings import DCM_REPORT_TEMPLATE
from service_example_settings import GBQ_DATASET
from service_example_settings import GBQ_TABLE
from service_example_settings import GCE_RUN_SCRIPT
from service_example_settings import GCS_BUCKET
from service_example_settings import SERVICE_NAME
from utils import TextUtils


CREDENTIAL_FILE = '../../ddmcredentials.dat'


def main():

  try:
    gcp = GCPConnector(PROJECT_ID)
    dcm = DCMConnector(
        credential_file=CREDENTIAL_FILE,
        user_email=None,
        profile_id=DCM_PROFILE_ID,
        api_version=DCM_API_VER)

    # In this example, we're mocking the config parameters (DCM partner and
    # advertiser IDs respectively):
    config_data = [['1234', '1111111'],
                   ['5678', '2222222']]

    for row in config_data:

      # Add params to be passed via task payload
      task_params = dict()
      task_params['service'] = SERVICE_NAME  # Mandatory field
      task_params['run_script'] = GCE_RUN_SCRIPT  # Mandatory field
      task_params['account_id'] = row[0]
      task_params['advertiser_id'] = row[1]
      task_params['bucket'] = GCS_BUCKET
      task_params['dataset'] = GBQ_DATASET

      # And add service-specific params
      task_params['report_template'] = DCM_REPORT_TEMPLATE
      task_params['report_name'] = DCM_REPORT_NAME
      task_params['date_range'] = DCM_REPORT_DATE_RANGE
      task_params['schema'] = DATA_SCHEMA_STANDARD
      task_params['filename'] = TextUtils.timestamp() + '_' + str(
          task_params['account_id']) + '.csv'
      task_params['table'] = GBQ_TABLE
      task_params['append'] = True

      service_example_run.service_task(dcm, gcp, task_params)

  # pylint: disable=broad-except
  except Exception as e:
    print e.message
  # pylint: enable=broad-except


if __name__ == '__main__':
  main()
