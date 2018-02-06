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

"""DNA - Service example - Main service module.

Main methods to handle the service tasks.
This example service uses DCM APIs (and the corresponding DNA connector) to
collect DCM report data, elaborate it, and push it to a BigQuery dataset.
"""

import base64
import json
import logging
import sys

from dcm_connector import DCMConnector
from dcm_connector import DCMReport
from dna_general_settings import CREDENTIAL_FILE
from dna_general_settings import DCM_API_VER
from dna_general_settings import GDS_KIND_LOG_SERVICE
from dna_logging import configure_logging
from dna_project_settings import DCM_PROFILE_ID
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector
from gcp_connector import GCPTable
from service_example_settings import DATA_SCHEMA_STANDARD
from service_example_settings import DCM_REPORT_DATE_RANGE
from service_example_settings import DCM_REPORT_NAME
from service_example_settings import DCM_REPORT_TEMPLATE
from service_example_settings import FIELD_MAP_STANDARD
from service_example_settings import GBQ_TABLE
from service_example_settings import SERVICE_NAME
from utils import TextUtils

# Configure logging
configure_logging()
logger = logging.getLogger('DNA-Service-example')


def get_field(field, row, field_map, default_value=None):
  """Access fields in a row according to the specified field map.

  Args:
    field: field to extract.
    row: row to extract field from.
    field_map: field map.
    default_value: value to be returned in case the field is not mapped.
  Returns:
    specified field value if the field is mapped, the default value otherwise.
  """

  field_info = field_map.get(field)

  if not field_info:
    return default_value
  else:
    return row[field_info['idx']]


def extract(dcm, params):
  """Create a DCM report and extract the resulting data.

  Args:
    dcm: initiated instance of the DCM connector.
    params: parameters to use for the DCM report.
  Returns:
    the data object resulting from the DCM report.
  """

  report = DCMReport(params['report_template'])
  report.setname(params['report_name'])
  report.setdates(params['date_range'])

  advertiser_ids = params['advertiser_id'].split(' ')

  # Add filters
  for item in advertiser_ids:
    report.addfilter('dfa:advertiser', item)

  # Insert and run a new report
  rid = dcm.createreport(report)
  fid = dcm.runreport(rid)

  # Get raw report data for the specified report and file
  data = dcm.getreportdata(rid, fid)

  # Delete the report from DCM
  dcm.deletereport(rid)

  return data


def transform(dest_table, raw_data):
  """Transform the report data and add it to the destination table.

  Args:
    dest_table: GCPConnector object.
    raw_data: GCPTable object containing the transformed data.
  """

  # Enable smart completion for the GCPTable object.
  assert isinstance(dest_table, GCPTable)

  # Loop over raw data rows (excluding header row).
  for row in raw_data[1:]:
    try:
      # Add all fields to the destination table
      dest_table.addrow([
          get_field('Advertiser', row, FIELD_MAP_STANDARD),
          get_field('AdvertiserID', row, FIELD_MAP_STANDARD),
          get_field('Campaign', row, FIELD_MAP_STANDARD),
          get_field('CampaignID', row, FIELD_MAP_STANDARD),
          get_field('PlacementSize', row, FIELD_MAP_STANDARD),
          get_field('CreativeType', row, FIELD_MAP_STANDARD),
          get_field('CreativeSize', row, FIELD_MAP_STANDARD),
          get_field('PlatformType', row, FIELD_MAP_STANDARD),
          get_field('Site', row, FIELD_MAP_STANDARD),
          get_field('Month', row, FIELD_MAP_STANDARD),
          get_field('Week', row, FIELD_MAP_STANDARD),
          get_field('Date', row, FIELD_MAP_STANDARD),
          int(get_field('Clicks', row, FIELD_MAP_STANDARD)),
          int(get_field('Impressions', row, FIELD_MAP_STANDARD)),
          float(get_field('ViewableTimeSeconds', row, FIELD_MAP_STANDARD)),
          int(get_field('EligibleImpressions', row, FIELD_MAP_STANDARD)),
          int(get_field('MeasurableImpressions', row, FIELD_MAP_STANDARD)),
          int(get_field('ViewableImpressions', row, FIELD_MAP_STANDARD)),
      ])
    # pylint: disable=broad-except
    except Exception as e:
      logger.debug('[%s] - Error "%s" occurs while adding the following row',
                   SERVICE_NAME, str(e))
      logger.debug(str(row))
    # pylint: enable=broad-except


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

  # Upload data onto a specified Google Cloud Storage bucket/filename
  gcsuri = gcp.gcs_uploadtable(source_table, bucket, filename)

  # Create a BigQuery job to transfer uploaded data from GCS to a BigQuery table
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


def service_task(dcm, gcp, params):
  """Main ETL job, putting together all ETL functions to implement the service.

  Args:
    dcm: the DCMConnector instance.
    gcp: the GCPConnector instance.
    params: dictionary containing all parameters relevant for the ETL task.
  Returns:
    The BigQuery job id.
  """

  # Initiate a GCPTable object (to be used for data ingestion) using the
  # appropriate data schema.
  dest_table = GCPTable(params['schema'])

  # Extract data via a DCM report.
  raw_data = extract(dcm, params)

  # Transform data if necessary (and upload it to the destination table)
  transform(dest_table, raw_data)

  # ...or alternatively ingest extracted data as it is
  # dest_table.ingest(raw_data, True)

  # Load data into Google Big Query.
  job_id = load(gcp, dest_table, params)

  return job_id


def main(argv):
  """Main function reading the task and launching the corresponding ETL job.

  Args:
    argv: array of parameters: (1) queue name, (2) task id.
  """

  # Get input arguments passed by the service-example-run.sh script
  queue_name = str(argv[1])
  task_name = str(argv[2])

  logger.info('Starting service-example processing task. Queue name: [%s]. '
              'Task name: [%s]', queue_name, task_name)

  # Initiate connectors for Google Cloud Platform and DCM.
  gcp = GCPConnector(PROJECT_ID)
  dcm = DCMConnector(
      credential_file=CREDENTIAL_FILE,
      user_email=None,
      profile_id=DCM_PROFILE_ID,
      api_version=DCM_API_VER)

  # Get the first available task from the queue.
  task = gcp.gct_gettask(task_name)
  payload = task['pullMessage']['payload']
  params = json.loads(base64.urlsafe_b64decode(str(payload)))

  # Add service-specific params.
  params['report_template'] = DCM_REPORT_TEMPLATE
  params['report_name'] = DCM_REPORT_NAME
  params['date_range'] = DCM_REPORT_DATE_RANGE
  params['schema'] = DATA_SCHEMA_STANDARD
  params['filename'] = TextUtils.timestamp() + '_' + str(
      params['account_id']) + '.csv'
  params['table'] = GBQ_TABLE
  params['append'] = False

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
    # Run the ETL task with the given params and update the Datastore entity.
    job_id = service_task(dcm, gcp, params)
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
