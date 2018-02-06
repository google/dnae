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

import dna_gae_handlers
# Import your "GAE handlers" file (do not remove this comment):
# import service_example_gae_handlers

import webapp2


class MainHandler(webapp2.RequestHandler):

  def get(self):
    self.response.write('Welcome to your DDM Network Analysis GAE app!')


app = webapp2.WSGIApplication([
    ('/', MainHandler),
    # Common Cron Jobs
    ('/core/cron/cleanup/compute', dna_gae_handlers.ComputeEngineCleanUp),
    ('/core/cron/cleanup/datastore', dna_gae_handlers.DatastoreCleanUp),
    ('/core/cron/cleanup/storage', dna_gae_handlers.CloudStorageCleanUp),
    ('/core/cron/check/bqjobs', dna_gae_handlers.BigQueryJobStatusCheck),
    ('/core/cron/compute', dna_gae_handlers.TaskManager),
    # Services
    # Add Cron Jobs for your services (do not remove this comment):
    # ('/services/service-example/run',
    #  service_example_gae_handlers.ServiceExampleLauncher)
], debug=True)


