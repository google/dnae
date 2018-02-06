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

"""DNA - Service Setup script.

This interactive script helps setting up a DNA service, automating (or partially
automating) the following steps:
- creation of the folder with the service files
- copy of the templated service files, replacing the placeholder values
- update of the project files adding references to the new service
"""

import errno
import os
import sys
from shutil import copyfile

sys.path.append('lib/core')
from dna_project_settings import GCS_PROJECT_ROOT
from dna_project_setup import BACKUP_MAIN_FOLDER

SERVICES_FOLDER = 'services/'
SERVICE_TEMPLATE_FOLDER = SERVICES_FOLDER + 'service-template/'
SERVICE_TEMPLATE_PREFIX = 'service_template'
SUFFIX_RUN_SCRIPT = '-run.sh'
SUFFIX_GAE_HANDLERS = '_gae_handlers.py'
SUFFIX_RUN_FILE = '_run.py'
SUFFIX_SETTINGS_FILE = '_settings.py'
SUFFIX_TEST_FILE = '_test.py'

APPENGINE_CONFIG_FILE = 'appengine_config.py'
APPENGINE_MAIN_FILE = 'appengine_main.py'
APPENGINE_MAIN_PLACEHOLDER1 = ('# Import your "GAE handlers" file (do not '
                               'remove this comment):')
APPENGINE_MAIN_PLACEHOLDER2 = ('# Add Cron Jobs for your services (do not '
                               'remove this comment):')
IMPORT_ENTRY_TEMPLATE = 'import %s_gae_handlers'
CRON_URL_TEMPLATE = "    ('/services/%s/run', %s_gae_handlers.%sLauncher),"
CRON_FILE = 'cron.yaml'
CRON_ENTRY_TEMPLATE = ('- description: %s\n  url: /services/%s/run\n  '
                       'schedule: every day 05:00\n  timezone: Europe/Rome')
DEPLOY_FILE = 'deploy.sh'
DEPLOY_PLACEHOLDER = '# Deploy your service files (do not remove this comment):'
DEPLOY_ENTRY_TEMPLATE = ("gsutil -m cp './services/%s/*.*' "
                         "'gs://%s-python-sources/'")
FILES_TO_BACKUP = [
    APPENGINE_CONFIG_FILE, APPENGINE_MAIN_FILE, CRON_FILE, DEPLOY_FILE
]


def replace_service_name(filepath, service_name):
  """Replaces "service-template" placeholders with the actual service name."""
  with open(filepath, 'r') as original_file:
    filedata = original_file.read()
  service_name_dash = service_name.replace('_', '-')
  service_launcher_name = service_name.title().replace('_', '')
  filedata = (filedata.replace('service_template', service_name)
              .replace('service-template', service_name_dash)
              .replace('SERVICE-TEMPLATE', service_name_dash.upper())
              .replace('ServiceTemplate', service_launcher_name))
  with open(filepath, 'w') as original_file:
    original_file.write(filedata)


def main():

  print '\n*******************************************'
  print '* Welcome do the DNA service setup script *'
  print '*******************************************'

  print "\nThis script will help you configure an 'empty' DNA service"
  print "creating the folder and the files you'll need to start."
  print('\nPlease insert the name you want to give to the service, only using '
        'lowercase letters and with multiple')
  service_name = raw_input(
      "words separated by an underscore (e.g. 'my_service'): ").lower().replace(
          '-', '_')
  service_name_dash = service_name.replace('_', '-')
  service_folder = SERVICES_FOLDER + service_name_dash

  # Creates the service folder
  try:
    os.makedirs(service_folder)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise
  print 'Created service (sub)folder: %s' % service_folder

  # Creates copies of the template files
  print 'Creating files:'
  run_script = service_folder + '/' + service_name_dash + SUFFIX_RUN_SCRIPT
  copyfile(SERVICE_TEMPLATE_FOLDER + SERVICE_TEMPLATE_PREFIX.replace('_', '-') +
           SUFFIX_RUN_SCRIPT, run_script)
  replace_service_name(run_script, service_name)
  print '- %s' % run_script
  gae_handlers = service_folder + '/' + service_name + SUFFIX_GAE_HANDLERS
  copyfile(
      SERVICE_TEMPLATE_FOLDER + SERVICE_TEMPLATE_PREFIX + SUFFIX_GAE_HANDLERS,
      gae_handlers)
  replace_service_name(gae_handlers, service_name)
  print '- %s' % gae_handlers
  run_file = service_folder + '/' + service_name + SUFFIX_RUN_FILE
  copyfile(SERVICE_TEMPLATE_FOLDER + SERVICE_TEMPLATE_PREFIX + SUFFIX_RUN_FILE,
           run_file)
  replace_service_name(run_file, service_name)
  print '- %s' % run_file
  settings_file = service_folder + '/' + service_name + SUFFIX_SETTINGS_FILE
  copyfile(
      SERVICE_TEMPLATE_FOLDER + SERVICE_TEMPLATE_PREFIX + SUFFIX_SETTINGS_FILE,
      settings_file)
  replace_service_name(settings_file, service_name)
  print '- %s' % settings_file
  test_file = service_folder + '/' + service_name + SUFFIX_TEST_FILE
  copyfile(SERVICE_TEMPLATE_FOLDER + SERVICE_TEMPLATE_PREFIX + SUFFIX_TEST_FILE,
           test_file)
  replace_service_name(test_file, service_name)
  print '- %s' % test_file
  print '\nDone!!!'

  print ('\nThis script can also update the following files to include the '
         "references to your new service '%s'") % service_name
  print '- %s' % APPENGINE_CONFIG_FILE
  print '- %s' % APPENGINE_MAIN_FILE
  print '- %s' % CRON_FILE
  print '- %s' % DEPLOY_FILE
  update_scripts = (raw_input('Do you want to update these scripts too [y/n]?')
                    .upper())
  if update_scripts != 'Y':
    print 'Ok, goodbye!'
    exit(0)

  try:
    os.makedirs(BACKUP_MAIN_FOLDER)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise
  for filepath in FILES_TO_BACKUP:
    copyfile(filepath, BACKUP_MAIN_FOLDER + '/' + filepath)
  print 'Original files backed up in the %s folder' % BACKUP_MAIN_FOLDER
  print '\nUpdating %s...' % APPENGINE_CONFIG_FILE
  with open(APPENGINE_CONFIG_FILE, 'a+') as appengine_config_file:
    appengine_config_file.write("\nvendor.add('%s')" % service_folder)
    appengine_config_file.close()
  print '..done.'

  print 'Updating %s...' % APPENGINE_MAIN_FILE
  with open(APPENGINE_MAIN_FILE, 'r') as appengine_main_file:
    filedata = appengine_main_file.read()
    appengine_main_file.close()
  cron_url = CRON_URL_TEMPLATE % (service_name_dash, service_name,
                                  service_name.title().replace('_', ''))
  import_entry = IMPORT_ENTRY_TEMPLATE % service_name
  filedata = (filedata.replace(APPENGINE_MAIN_PLACEHOLDER1,
                               APPENGINE_MAIN_PLACEHOLDER1 + '\n' +
                               import_entry)
              .replace(APPENGINE_MAIN_PLACEHOLDER2,
                       APPENGINE_MAIN_PLACEHOLDER2 + '\n' + cron_url))
  with open(APPENGINE_MAIN_FILE, 'w') as appengine_main_file:
    appengine_main_file.write(filedata)
    appengine_main_file.close()
  print '..done.'

  print 'Updating %s...' % CRON_FILE
  service_readable = service_name.title().replace('_', ' ')
  cron_entry = CRON_ENTRY_TEMPLATE % (service_readable, service_name_dash)
  with open(CRON_FILE, 'a+') as cron_file:
    cron_file.write('\n\n' + cron_entry)
    cron_file.close()
  print '..done.'

  print 'Updating %s...' % DEPLOY_FILE
  deploy_entry = DEPLOY_ENTRY_TEMPLATE % (service_name_dash, GCS_PROJECT_ROOT)
  with open(DEPLOY_FILE, 'r') as deploy_file:
    filedata = deploy_file.read()
    deploy_file.close()
  filedata = filedata.replace(DEPLOY_PLACEHOLDER,
                              DEPLOY_PLACEHOLDER + '\n' + deploy_entry)
  with open(DEPLOY_FILE, 'w') as deploy_file:
    deploy_file.write(filedata)
    deploy_file.close()
  print '..done.'
  print ('\nPlease note that the cron job has currently been scheduled for a '
         'daily run at 5am - you might want ')
  print ('to adapt it to your preferred frequency directly in file \'%s.\''
         % CRON_FILE)
  print ('\nAlso, remember to create all relevant Google Cloud Storage '
         'buckets and BigQuery datasets for your')
  print ('service. In this example, the names for both are defined in file %s'
         % settings_file)
  print ('(likely \'%s\' and \'%s\' respectively)'
         % (GCS_PROJECT_ROOT + '-' + service_name_dash,
            service_name + '_dataset'))
  raw_input('\nThat\'s all, presse ENTER to finish!')
  exit(0)


if __name__ == '__main__':
  main()
