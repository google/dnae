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

"""DNA - Core AppEngine handlers.

The DNA framework comes with a set of pre-built GAE handlers to address common
needs like cleaning up Datastore entities and Storage buckets, deleting unused
VMs as well as core functionalities like the task manager.
"""

import datetime
import logging
import time
import uuid

from dna_general_settings import DNA_STATUS_CREATED
from dna_general_settings import DNA_STATUS_DONE
from dna_general_settings import DNA_STATUS_FAILED
from dna_general_settings import DNA_STATUS_RUNNING
from dna_general_settings import GCE_MACHINE_MAP
from dna_general_settings import GCE_MAX_STARTUP_TIME
from dna_general_settings import GCE_SCOPES
from dna_general_settings import GCE_SHUTDOWN_SCRIPT
from dna_general_settings import GCE_STARTUP_SCRIPT
from dna_general_settings import GDS_KIND_CE_INSTANCE
from dna_general_settings import GDS_KIND_CLEANUP_GCS
from dna_general_settings import GDS_KIND_LOG_SERVICE
from dna_project_settings import GCE_SERVICE_ACCOUNT
from dna_project_settings import GCS_PROJECT_ROOT
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector
from utils import TextUtils


def task_manager():
  """Task manager function.

  Looks for tasks in the DNA queues and launches CE instances accordingly.
  Returns:
      Standard 'OK' string to confirm completed execution.
  """
  gcp = GCPConnector(PROJECT_ID)
  for level in ['l0', 'l1', 'l2', 'l3']:

    zone = GCE_MACHINE_MAP[level]['zone']
    queue = GCE_MACHINE_MAP[level]['queue']
    quota = GCE_MACHINE_MAP[level]['quota']
    vm_type = GCE_MACHINE_MAP[level]['type']

    # Retrieve the list of existing instances if any
    num_running_ce = 0
    ce_list = gcp.gce_listinstances(zone)
    if 'items' in ce_list:
      for item in ce_list['items']:
        starting_idx = item['machineType'].find('machineTypes/')
        mtype = item['machineType'][starting_idx + 13:]
        if mtype == vm_type:
          num_running_ce += 1

    # Check how many tasks are in the queue and calculate the number of CE
    # machines to be created
    task_list = gcp.gct_listtasks(queue)
    logging.debug('%s elements in queue [%s]', len(task_list), queue)
    if 'tasks' in task_list:
      num_tasks = len(task_list['tasks'])
      pool_size = quota - num_running_ce
      pool_size = min(pool_size, num_tasks)
    else:
      logging.debug('No \'tasks\' in Cloud Task queue')
      # No tasks in the queue
      pool_size = 0
    logging.debug('Level: [%s]. Pool size: %s', level, pool_size)
    # Create a pool of CE instances if pool_size>0
    if pool_size > 0:
      for i in range(pool_size):
        machine_id = '%s-%s' % (level, str(i))
        instance_name = 'dna-machine-%s-%s' % (machine_id,
                                               str(uuid.uuid1().hex))
        logging.debug('Configuring new machine. ID: [%s]. Instance name: [%s]',
                      machine_id, instance_name)

        # Insert a new Datastore entry for each CE instance
        ce_entity = gcp.gds_insert(
            kind=GDS_KIND_CE_INSTANCE,
            attributes={
                'name': instance_name,
                'zone': zone,
                'created': TextUtils.timestamp(),
                't0': time.time(),
                'status': None
            })

        # Get the basic configuration as defined in the GCPConnector class
        ce_config = gcp.gce_configinstance(
            name=instance_name,
            zone=zone,
            machine_type=vm_type,
            service_account=GCE_SERVICE_ACCOUNT,
            scopes=GCE_SCOPES)

        # Add some metadata
        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='startup-script-url',
            value=GCE_STARTUP_SCRIPT)

        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='shutdown-script-url',
            value=GCE_SHUTDOWN_SCRIPT)

        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='machine-id',
            value=machine_id)

        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='ce-entity-id',
            value=str(ce_entity.key.id))

        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='project-root',
            value=GCS_PROJECT_ROOT)

        ce_config = gcp.gce_addmetadata(
            config=ce_config,
            key='level',
            value=level)

        # Create the instance
        gcp.gce_createinstance(ce_config)
        logging.debug('Instance created. Name: [%s]', instance_name)
        # Update the status of the corresponding Datastore entity
        ce_entity['status'] = DNA_STATUS_CREATED
        gcp.gds_update(ce_entity)
        logging.debug('CE DataStore entity updated')
  return 'OK'


def ce_cleanup():
  """Compute Engine Cleanup Handler.

    Delete CE instances that have completed their job.
  Returns:
      Standard 'OK' string to confirm completed execution.
  """
  gcp = GCPConnector(PROJECT_ID)
  ce_entities = gcp.gds_query(GDS_KIND_CE_INSTANCE)

  for entity in ce_entities:

    startup_failed = False

    if not entity['status']:
      # Some error creating the machine.
      logging.warning('[DNA-CLEANUP-COMPUTE] Failed creating machine "%s"',
                      entity['name'])
      gcp.gds_delete(entity)

    if entity['status'] == DNA_STATUS_CREATED:
      # Calculates seconds elapsed since the machine was created
      et = time.time() - entity['t0']
      if et > GCE_MAX_STARTUP_TIME:
        # After GCE_MAX_STARTUP_TIME seconds we assume that something went
        # wrong in the startup. Make sure the GCE_MAX_STARTUP_TIME is properly
        # calibrated according to your typical application startup time
        logging.warning(
            '[DNA-CLEANUP-COMPUTE] Startup Failed for machine "%s"',
            entity['name'])
        startup_failed = True

    if (entity['status'] == DNA_STATUS_DONE) or startup_failed:
      # If the CE instance is done or if the startup is failed,
      # the instance is deleted
      gcp.gce_deleteinstance(entity['name'], entity['zone'])
      gcp.gds_delete(entity)
  return 'OK'


def bq_job_status_check():
  """Check BigQuery Job Handler.

  Check BigQuery jobs status. To take advantage of this service, you have to
  provide the following fields in a Datastore entry:

  - bqjob    --> BigQuery job id
  - bqstatus --> if this field is equal to DNA_STATUS_RUNNING the service will
                 check the actual status of the job
  - bqerror  --> if an error occurs, the error message is reported here

  Returns:
      Standard 'OK' string to confirm completed execution.

  """
  gcp = GCPConnector(PROJECT_ID)
  ds_entities = gcp.gds_query(GDS_KIND_LOG_SERVICE)

  for entity in ds_entities:
    if 'bqstatus' in entity:
      if entity['bqstatus'] == DNA_STATUS_RUNNING:
        job_info = gcp.bq_getjobinfo(entity['bqjob'])
        entity['bqstatus'] = job_info['status']['state']
        if 'errorResult' in job_info['status']:
          entity['bqstatus'] = DNA_STATUS_FAILED
          entity['bqerror'] = str(job_info['status']['errors'])
        gcp.gds_update(entity)
  return 'OK'


def ds_cleanup():
  """Datastore Cleanup Handler.

  Remove all DNA-related Datastore entities.

  Returns:
      Standard 'OK' string to confirm completed execution.
  """
  gcp = GCPConnector(PROJECT_ID)
  ds_entities = gcp.gds_query(GDS_KIND_CE_INSTANCE)
  ds_entities += gcp.gds_query(GDS_KIND_LOG_SERVICE)
  for entity in ds_entities:
    gcp.gds_delete(entity)
  return 'OK'


def cs_cleanup():
  """Cloud Storage Cleanup Handler.

  Remove files older then LBW days from a specified GCS bucket.
  To take advantage of this service you have to define for each bucket a
  Datastore entity of kind GDS_KIND_CLEANUP_GCS and provide the following
  fields:

  - bucket --> name of the bucket to cleanup
  - lbw    --> lookback window

  Returns:
      Standard 'OK' string to confirm completed execution.

  """
  gcp = GCPConnector(PROJECT_ID)

  # GDS_KIND_CLEANUP_GCS entities in Datastore describes which GCS buckets
  # have to be cleaned
  cleanup_entities = gcp.gds_query(GDS_KIND_CLEANUP_GCS)

  for entity in cleanup_entities:
    for blob in gcp.gcs_getblobs(entity['bucket']):
      if blob.updated.date().toordinal(
      ) < datetime.date.today().toordinal() - entity['lbw']:
        try:
          blob.delete()
          logging.info('[DNA-CLEANUP-GCS:%s] File deleted', blob.name)
        # pylint: disable=broad-except
        except Exception as e:
          logging.warning('[DNA-CLEANUP-GCS:%s] Something went wrong - %s',
                          blob.name,
                          str(e))
        # pylint: enable=broad-except
  return 'OK'
