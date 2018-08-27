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

"""DNA - Main AppEngine module.

Main routing configuration for the AppEngine application.
"""

import sys
sys.path.append('lib/connectors')
sys.path.append('lib/core')
sys.path.append('lib/utils')
# Add reference to your service folder (do not remove this comment):
# sys.path.append('services/service_example')

import dna_gae_handlers
# Import your "GAE handlers" file (do not remove this comment):
# import service_example_gae_handlers

from flask import Flask

app = Flask(__name__)


@app.route('/')
def root():
  return 'Welcome to DNAE!'


@app.route('/core/cron/check/bqjobs')
def bq_job_status_check():
  return dna_gae_handlers.bq_job_status_check()


@app.route('/core/cron/cleanup/compute')
def compute_engine_cleanup():
  return dna_gae_handlers.ce_cleanup()


@app.route('/core/cron/cleanup/datastore')
def datastore_cleanup():
  return dna_gae_handlers.ds_cleanup()


@app.route('/core/cron/cleanup/storage')
def cloud_storage_cleanup():
  return dna_gae_handlers.cs_cleanup()


@app.route('/core/cron/compute')
def task_manager():
  return dna_gae_handlers.task_manager()


# Services
# Add routing rules for your services (do not remove this comment):
# @app.route('/services/service-example/run')
# def service_example_launcher():
#   return service_example_gae_handlers.service_example_launcher()

if __name__ == '__main__':
  # This is used when running locally only.
  app.run(host='127.0.0.1', port=8080, debug=True)


