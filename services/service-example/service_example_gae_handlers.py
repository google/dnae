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

"""DNA - Service example - App Engine handlers.

App Engine handler definitions for the service, including the main "launcher".
"""

import base64
import json

from dna_general_settings import GCE_MACHINE_MAP
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector
from service_example_settings import GBQ_DATASET
from service_example_settings import GCE_RUN_SCRIPT
from service_example_settings import GCS_BUCKET
from service_example_settings import SERVICE_NAME


def service_template_test_launcher():
  gcp = GCPConnector(PROJECT_ID)
  queue_name = GCE_MACHINE_MAP['l1']['queue']

  # In this example, we're mocking the config parameters (DCM partner and
  # advertiser IDs respectively):
  config_data = [['1234', '1111111'],
                 ['5678', '2222222']]

  # In a real life scenario, you might want to use an external data source,
  # such as a Google Sheet doc through the Sheet connector:
  # config_doc = GSheetConnector(CREDENTIAL_FILE)
  # config_data = config_doc.getvalues(SERVICE_CONFIG_SHEET_ID, 'Setup!A:B')

  for row in config_data:
    # Add params to be passed via task payload
    task_params = dict()
    task_params['service'] = SERVICE_NAME  # Mandatory field
    task_params['run_script'] = GCE_RUN_SCRIPT  # Mandatory field
    task_params['advertiser_id'] = row[0]
    task_params['label'] = row[1]
    task_params['bucket'] = GCS_BUCKET
    task_params['dataset'] = GBQ_DATASET

    # Add a new task to the task queue
    string_payload = json.dumps(task_params)
    base64_payload = base64.urlsafe_b64encode(string_payload.encode())
    payload = base64_payload.decode()
    gcp.gct_createtask(queue_name, payload)
  return 'OK'
