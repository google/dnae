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

"""DNA - Service template - Settings constants."""

from dna_project_settings import GCS_PROJECT_ROOT

SERVICE_NAME = "SERVICE-TEMPLATE"

# BigQuery Dataset and table names.
GBQ_DATASET = "service_template_dataset"
GBQ_TABLE = "service_template_table"
# Cloud Storage bucket name.
GCS_BUCKET = "%s-service-template" % GCS_PROJECT_ROOT
# Main script to run on the Compute Engine instance.
GCE_RUN_SCRIPT = "./service-template-run.sh"

# Service-specific data schemas.
DATA_SCHEMA = {
    "fields": [
        {
            "name": "AccountID",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Label",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "RandomValue",
            "type": "STRING",
            "mode": "NULLABLE"
        },
    ]
}
