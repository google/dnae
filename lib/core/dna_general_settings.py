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

"""DNA - General Settings.

General constants and dictionaries definitions.
"""

from dna_project_settings import GCS_PROJECT_ROOT

# API Versions
DCM_API_VER = 'v2.8'

# Credentials
CREDENTIAL_FILE = 'ddmcredentials.dat'

# Status constant values
DNA_STATUS_ACTIVE = u'ACTIVE'
DNA_STATUS_CREATED = u'CREATED'
DNA_STATUS_DONE = u'DONE'
DNA_STATUS_FAILED = u'FAILED'
DNA_STATUS_RUNNING = u'RUNNING'

# Cloud Datastore entity kinds definition
GDS_KIND_CE_INSTANCE = 'DNAInstance'
GDS_KIND_LOG_SERVICE = 'DNALogService'
GDS_KIND_CLEANUP_GCS = 'DNACleanUpGCS'

# Cloud Compute Engine machines mapping
GCE_MACHINE_MAP = dict()
GCE_MACHINE_MAP['l0'] = {
    'type': 'custom-1-6656',
    'zone': 'europe-west1-b',
    'queue': 'dna-tasks-l0',
    'quota': 8  # Because the quota limit for IP addresses is 8
}
GCE_MACHINE_MAP['l1'] = {
    'type': 'n1-highmem-2',
    'zone': 'europe-west2-b',
    'queue': 'dna-tasks-l1',
    'quota': 8
}
GCE_MACHINE_MAP['l2'] = {
    'type': 'n1-highmem-4',
    'zone': 'europe-west3-b',
    'queue': 'dna-tasks-l2',
    'quota': 4
}
GCE_MACHINE_MAP['l3'] = {
    'type': 'n1-highmem-8',
    'zone': 'europe-west3-b',
    'queue': 'dna-tasks-l3',
    'quota': 1
}

# DoubleClick API scopes typically used in DNA projects
DBM_SCOPE = [
    'https://www.googleapis.com/auth/doubleclickbidmanager'
]
DCM_SCOPES = [
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking'
]
DS_SCOPE = [
    'https://www.googleapis.com/auth/doubleclicksearch'
]

# Google Cloud API scopes used by the DNA core framework
GCE_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform', 'placeholder_dbm',
    'placeholder_dcm', 'placeholder_ds',
    'https://www.googleapis.com/auth/taskqueue',
    'https://www.googleapis.com/auth/taskqueue.consumer'
]

# Bash scripts used by each CE instance at start-up and shut-down
GCE_STARTUP_SCRIPT = 'gs://%s-bash-scripts/dna-compute-startup.sh' % GCS_PROJECT_ROOT
GCE_SHUTDOWN_SCRIPT = 'gs://%s-bash-scripts/dna-compute-shutdown.sh' % GCS_PROJECT_ROOT

# Max start-up time (in seconds) allowed before deleting the CE instance
GCE_MAX_STARTUP_TIME = 3*60
