#!/usr/bin/env bash
#
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
#
# Deploys all necessary files to Cloud Storage and App Engine.

# Remove all files from the destination bucket
gsutil -m rm -rf 'gs://placeholder-python-sources/*'

# Internal libraries (and external libraries requirement file) deployment
gsutil -m cp './lib/connectors/*.py' 'gs://placeholder-python-sources/'
gsutil -m cp './lib/utils/*.py' 'gs://placeholder-python-sources/'
gsutil -m cp './requirements.txt' 'gs://placeholder-python-sources/'

# Credentials file deployment
gsutil -m cp './ddmcredentials.dat' 'gs://placeholder-python-sources/'

# Core files deployment
gsutil -m cp './lib/core/dna_general_settings.py' 'gs://placeholder-python-sources/'
gsutil -m cp './lib/core/dna_logging.py' 'gs://placeholder-python-sources/'
gsutil -m cp './lib/core/dna_project_settings.py' 'gs://placeholder-python-sources/'
gsutil -m cp './lib/core/dna_compute_main.py' 'gs://placeholder-python-sources/'
gsutil -m cp './lib/core/dna-compute-startup.sh' 'gs://placeholder-bash-scripts/'
gsutil -m cp './lib/core/dna-compute-shutdown.sh' 'gs://placeholder-bash-scripts/'

# Services files deployment
# Deploy your service files (do not remove this comment):
# gsutil -m cp './services/service-example/*.*' 'gs://placeholder-python-sources/'

# App Engine Deployment
yes | gcloud app deploy --project=placeholder-project-id --version=v1
yes | gcloud app deploy --project=placeholder-project-id cron.yaml
yes | gcloud app deploy --project=placeholder-project-id queue.yaml