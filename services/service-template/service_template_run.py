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

"""DNA - Service template - Main service module.

Main methods to handle the service tasks.
"""

import base64
import json
import logging
import random
import sys

from dna_general_settings import GDS_KIND_LOG_SERVICE
from dna_logging import configure_logging
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector
from gcp_connector import GCPTable
from service_template_settings import DATA_SCHEMA
from service_template_settings import GBQ_DATASET
from service_template_settings import GBQ_TABLE
from service_template_settings import SERVICE_NAME
from utils import TextUtils

configure_logging()
logger = logging.getLogger('service-template')


def load(gcp, source_table, params):
  """Load transformed data onto Google Cloud Platform.

  Args:
    gcp: GCPConnector object.
    source_table: GCPTable object containing the transformed data.
    params: dictionary containing all relevant GCP parameters.
  Returns:
    The BigQuery job id.
  """
  assert isinstance(gcp, GCPConnector)
  assert isinstance(source_table, GCPTable)

  bucket = params['bucket']
  filename = params['filename']
  dataset = params['dataset']
  table = params['table']

  # Upload data onto a specified Google Cloud Storage bucket/filename.
  gcsuri = gcp.gcs_uploadtable(source_table, bucket, filename)

  # Create a BigQuery job to transfer uploaded data from GCS to a BQ table.
  if params['append']:
    # If append is "True" append transformed data to the table
    job_id = gcp.bq_importfromgcs(
        gcsuri=gcsuri,
        dataset=dataset,
        table=table,
        schema=source_table.schema,
        encoding=source_table.encoding,
        writemode='WRITE_APPEND')
  else:
    # Otherwise overwrite (or create a new) table
    job_id = gcp.bq_importfromgcs(
        gcsuri=gcsuri,
        dataset=dataset,
        table=table,
        schema=source_table.schema,
        encoding=source_table.encoding)
  return job_id


def service_task(gcp, params):
  """Main ETL job, putting together all ETL functions to implement the service.

  Args:
    gcp: GCPConnector object.
    params: dictionary containing all parameters relevant for the ETL task.
  Returns:
    The BigQuery job id.
  """

  # Initiate a GCPTable object (to be used for data ingestion) using the
  # appropriate data schema.
  dest_table = GCPTable(params['schema'])

  # Extract (and eventually transform) data - in this template we're just using
  # dummy random data.
  random_number = random.randint(1, 10)
  raw_data = [[params['account_id'], params['label'], random_number]]

  # Ingest the data.
  dest_table.ingest(raw_data, True, False)

  # Load data to GCP.
  job_id = load(gcp, dest_table, params)

  return job_id


def main(argv):
  """Main function reading the task and launching the corresponding ETL job.

  Args:
    argv: array of parameters: (1) queue name, (2) task id.
  """

  # Get input arguments passed by the initial shell script.
  queue_name = str(argv[1])
  task_name = str(argv[2])

  # Initiate connectors for Google Cloud Platform (and DCM/DBM/DS as needed).
  gcp = GCPConnector(PROJECT_ID)

  # Get the first available task from the queue.
  task = gcp.gct_gettask(task_name)
  payload = task['pullMessage']['payload']
  params = json.loads(base64.urlsafe_b64decode(str(payload)))

  # Add service-specific params.
  params['schema'] = DATA_SCHEMA
  params['filename'] = TextUtils.timestamp() + '_' + str(
      params['account_id']) + '.csv'
  params['dataset'] = GBQ_DATASET
  params['table'] = GBQ_TABLE
  params['append'] = True

  # Log run info as Datastore entity.
  run_entity = gcp.gds_insert(
      kind=GDS_KIND_LOG_SERVICE,
      attributes={
          'created': TextUtils.timestamp().decode(),
          'service': params['service'].decode(),
          'status': u'RUNNING',
          'error': None,
          'bqjob': None,
          'bqstatus': None,
      })
  try:
    # Run the ETL task and updates the Datastore entity status.
    job_id = service_task(gcp, params)
    run_entity['bqjob'] = job_id.decode()
    run_entity['bqstatus'] = u'RUNNING'
    run_entity['status'] = u'DONE'

  # pylint: disable=broad-except
  except Exception as e:
    run_entity['status'] = u'FAILED'
    run_entity['error'] = str(e).decode()
    logger.error(
        '[%s] - The following error occurs while executing task <%s> : <%s>',
        SERVICE_NAME, task_name, str(e))
  finally:
    gcp.gds_update(run_entity)
  # pylint: enable=broad-except


if __name__ == '__main__':
  main(sys.argv)
