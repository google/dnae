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

"""DNA - Main Compute Engine instances module.

Main loop running on CE instances responsibile for actual task execution.
"""

import base64
import json
import logging
import subprocess
import sys

from dna_general_settings import DNA_STATUS_ACTIVE
from dna_general_settings import DNA_STATUS_DONE
from dna_general_settings import GCE_MACHINE_MAP
from dna_general_settings import GDS_KIND_CE_INSTANCE
from dna_logging import configure_logging
from dna_project_settings import PROJECT_ID
from gcp_connector import GCPConnector


def main(argv):

  # Configure logger
  configure_logging()
  logger = logging.getLogger('DNA-compute')

  # Get input arguments passed by the dna-compute-startup.sh script
  level = str(argv[1])
  ce_entity_id = str(argv[2])

  logger.info('DNA compute main started. Level: <%s>. CE Entity: <%s>',
              level, ce_entity_id)
  # Get the name of the queue to be consumed by this machine based on level
  queue_name = GCE_MACHINE_MAP[level]['queue']

  # Initiate a connector to GCP
  gcp = GCPConnector(PROJECT_ID)

  # Retrieve the datastore entity that stores the status of this machine
  ce_entity = gcp.gds_querybyid(GDS_KIND_CE_INSTANCE, long(ce_entity_id))
  ce_entity['status'] = DNA_STATUS_ACTIVE
  gcp.gds_update(ce_entity)

  # If something goes wrong before the "gcp.gds_update(ce_entity)" call, the
  # status of the machine will remain DNA_STATUS_CREATING and the DNA Compute
  # Clean-up service will delete the machine after the GCE_MAX_STARTUP_TIME will
  # be elapsed

  # Start processing tasks
  task_in_queue = True

  while task_in_queue:

    service = 'UNKNOWN'

    try:
      # The loop will end if there is no more "items" in the queue or if
      # something goes wrong in the "gcp.gct_leasetask" call
      task_in_queue = False

      # Lease the first available task for enough time to make it done
      res = gcp.gct_leasetask(queue_name, 2*3600, 1)

      if 'tasks' in res:
        task_in_queue = True
        task = res['tasks'][0]
        task_name = task['name']
        schedule_time = task['scheduleTime']
        payload = task['pullMessage']['payload']
        payloadstr = base64.urlsafe_b64decode(str(payload))
        params = json.loads(payloadstr)
        service = str(params['service'])

        try:
          logger.info('[%s] - Task <%s> started', service, task_name)
          subprocess.check_call(['sh', params['run_script'],
                                 queue_name, task_name])
          logger.info(
              '[%s] - Task <%s> successfully completed - Payload <%s>',
              service,
              task_name,
              payloadstr)
          gcp.gct_acknowledgetask(task_name, schedule_time)
          logger.info('[%s] - Task <%s> ack\'d/deleted', service, task_name)

        except subprocess.CalledProcessError as err:

          next_level = None
          if err.returncode == 137:  # out of memory
            if level == 'l0':
              next_level = 'l1'
            elif level == 'l1':
              next_level = 'l2'
            elif level == 'l2':
              next_level = 'l3'

          if next_level:
            gcp.gct_acknowledgetask(task_name, schedule_time)
            logger.info('[%s] - Task <%s> ack\'d/deleted', service, task_name)
            next_level_queue = GCE_MACHINE_MAP[next_level]['queue']
            gcp.gct_createtask(next_level_queue, payload)
            logger.warning(
                '[%s] - Task <%s> payload moved from level <%s> to level <%s>'
                ' - Payload <%s>',
                service,
                task_name,
                level,
                next_level,
                payloadstr)
          else:
            raise err

    # pylint: disable=broad-except
    except Exception as err:
      logger.error(
          '[%s] - The following error occurs while executing a task : <%s>',
          service,
          str(err))
    # pylint: enable=broad-except

  if ce_entity:
    ce_entity['status'] = DNA_STATUS_DONE
    gcp.gds_update(ce_entity)


if __name__ == '__main__':
  main(sys.argv)
