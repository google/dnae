#!/usr/bin/python
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

"""DNA - Project Setup script.

This interactive script helps setting up a DNA project, automating (or partially
automating) the following steps:
- choice of the Google Cloud Platform project to use
- enablement of the AppEngine app
- choice of APIs to enable
- credential file generation
- update of template files replacing placeholders with project-specific values
- creation of Cloud Storage buckets
- service accounts definition
"""

import errno
import os
from shutil import copyfile

from gcloud import exceptions
from gcloud import storage
import generate_credential_file
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


PROJECT_ID = 'projectId'
PROJECT_NAME = 'name'
BACKUP_MAIN_FOLDER = 'setup_backup'
BACKUP_CORE_FOLDER = 'setup_backup/lib/core'
BACKUP_FOLDERS = [BACKUP_MAIN_FOLDER, BACKUP_CORE_FOLDER]
PYTHON_SOURCES_SUFFIX = '-python-sources'
BASH_SCRIPTS_SUFFIX = '-bash-scripts'
GENERAL_SETTINGS_FILE = 'lib/core/dna_general_settings.py'
PROJECT_SETTINGS_FILE = 'lib/core/dna_project_settings.py'
QUEUE_FILE = 'queue.yaml'
DEPLOY_FILE = 'deploy.sh'
FILES_TO_BACKUP = [
    GENERAL_SETTINGS_FILE, PROJECT_SETTINGS_FILE, QUEUE_FILE, DEPLOY_FILE
]


def storage_create_bucket(project_id, bucket_name):
  storage_client = storage.Client(project_id)
  bucket = storage_client.create_bucket(bucket_name)
  return 'Bucket {} created'.format(bucket.name)


def main():
  credentials = GoogleCredentials.get_application_default()
  service = discovery.build(
      'cloudresourcemanager', 'v1', credentials=credentials)

  print '***********************************'
  print '* Welcome do the DNA setup script *'
  print '***********************************'

  print '\n***** STEP 0: Backing up the current files ******'
  for folder_name in BACKUP_FOLDERS:
    try:
      os.makedirs(folder_name)
    except OSError as exception:
      if exception.errno != errno.EEXIST:
        raise
  for filepath in FILES_TO_BACKUP:
    copyfile(filepath, BACKUP_MAIN_FOLDER + '/' + filepath)
  print '..done (backup files ready in the %s folder)' % BACKUP_MAIN_FOLDER
  print('run \'dna_restore_backup.py\' to restore the backed up files if this '
        'tool somehow fails')

  print '\n***** STEP 1: Select the Google Cloud Project to use'
  print('\nSelect the project to use from the list of your Google Cloud '
        'projects below:')
  request = service.projects().list()
  response = []
  while request is not None:
    response = request.execute()
    for project in response['projects']:
      print str(
          response['projects'].index(project)) + ') ' + project[PROJECT_NAME]
    request = service.projects().list_next(
        previous_request=request, previous_response=response)
  project_index = raw_input(
      '\nChoose the project using the index (not the name): ')
  chosen_project = response['projects'][int(project_index)]
  project_id = chosen_project[PROJECT_ID]

  print('\nIf you don\'t have enabled a Python AppEngine app in your project, '
        'do it now:')
  print(
      '- Go to this link: '
      'https://console.cloud.google.com/appengine/start?project=%s'
      % project_id)
  print('- Follow the steps to activate your first app using Python as '
        'language.')
  print '- This might take a while, but you can skip the Tutorial'
  print '  (refer to Google Cloud documentation for more information)'
  print '- Please note that if you choose a different region than "US Central"'
  print '  for your AppEngine instance you\'ll need to update the gae_location'
  print '  variable in "lib/connectors/gcp_connector.py" accordingly'
  app_engine_active = raw_input(
      '\nDo you have your Python AppEngine app ready? [y/n]? ').upper()
  if app_engine_active != 'Y':
    print 'Fix that and run this script again!'
    exit(0)

  print '\n***** STEP 2: enable relevant APIs in your Google Cloud project'
  print 'Open the API Manager page for your project at this link:'
  print 'https://console.cloud.google.com/apis/library?project=%s' % project_id
  print 'and enable these APIs needed by the tool:'
  print '- Compute Engine API'
  print '- Cloud Tasks API (might need to get your account whitelisted)'
  print 'Also enable all the DoubleClick APIs you\'ll need to use:'
  print '- DCM/DFA Reporting And Trafficking API'
  print '- DoubleClick Search API'
  print '- DoubleClick Bid Manager API'
  print('- Google Sheets API (e.g. if you\'re going to use a spreadsheet to '
        'configure inputs)')
  proceed = raw_input('\nHave you enabled the APIs and are you ready to '
                      'proceed [y/n]? ').upper()
  if proceed != 'Y':
    print 'Goodbye!'
    exit(0)

  print '\n***** STEP 3: Generate your credentials file'
  print 'Now create a new Credential file following these steps:'
  print('- Go to the Credentials page: '
        'https://console.cloud.google.com/apis/credentials?project=%s'
       ) % project_id
  print('- Create a new Credential, choosing type \'OAuth Client ID\' and '
        'application type \'Other\'')
  print('- download the resulting JSON file clicking on the small download '
        'icon at the right end of your credential entry')
  print('  and save it with the exact filename \'client_secret.json\' in the '
        'same folder of this script')
  print '  (' + os.getcwd() + ')'
  proceed = raw_input(
      '\nHave you saved the client secrets file and are you ready to proceed '
      '[y/n]? ').upper()
  if proceed != 'Y':
    print 'Goodbye!'
    exit(0)

  uses_dbm = raw_input('\nAre you going to need the DBM API [y/n]? ').upper()
  uses_dcm = raw_input('\nAre you going to need the DCM API [y/n]? ').upper()
  uses_ds = raw_input('\nAre you going to need the DS API [y/n]? ').upper()
  uses_sheets = raw_input(
      '\nAre you going to need the Sheets API (e.g. to configure inputs via '
      'Spreadsheets) [y/n]? ').upper()
  print('\nWe are now going to create your credentials file -  if necessary it'
        ' will open a window of your browser for you to authorize access '
        'through your Google Account.')
  proceed = raw_input('Enter \'y\' when you\'re ready to proceed: ').upper()
  if proceed != 'Y':
    print 'Goodbye!'
    exit(0)

  generate_credential_file.from_setup(uses_dbm, uses_dcm, uses_ds, uses_sheets)
  print '\n***** STEP 4: Update the template files with your project details)'
  print('\nChoose a short name for your DNA setup, which will be used as '
        'prefix for the Cloud Storage bucket folders and for the BigQuery '
        'Datasets.')
  print 'Allowed characters: lowercase a-z, 0-9, and the dash \'-\''
  print 'Examples: \'my-dna\', \'dna-italy\', \'project-abcd\'.'
  project_prefix = raw_input('Your choice for the name/prefix: ').lower()

  print '\nCreating the required Cloud Storage buckets for %s' % project_prefix
  python_sources_bucket = project_prefix + PYTHON_SOURCES_SUFFIX
  try:
    storage_create_bucket(project_id, python_sources_bucket)
  except exceptions.Conflict:
    choice = raw_input(
        'Folder %s already exists, do you want to use it (y) or quit (n)? ' %
        python_sources_bucket).upper()
    if choice == 'N':
      exit(1)
  print '%s...done' % python_sources_bucket
  bash_scripts_bucket = project_prefix + BASH_SCRIPTS_SUFFIX
  try:
    storage_create_bucket(project_id, bash_scripts_bucket)
  except exceptions.Conflict:
    choice = raw_input(
        'Folder %s already exists, do you want to use it (y) or quit (n)? ' %
        bash_scripts_bucket).upper()
    if choice == 'N':
      exit(1)
  print '%s...done' % bash_scripts_bucket

  print('\nOpen this URL: '
        'https://console.cloud.google.com/iam-admin/serviceaccounts/'
        'project?project=%s') % project_id
  app_engine_sa = raw_input(
      'and paste here the Service account ID for the \'App Engine default '
      'service account\': ')
  compute_engine_sa = raw_input(
      'And the one for the \'Compute Engine default service account\': ')
  user_email = raw_input('Lastly, enter your Google account email: ')
  dcm_profile_id = 'No DCM access needed'
  if uses_dcm == 'Y':
    dcm_profile_id = raw_input('\nEnter your DCM Profile ID to use with the DCM'
                               ' API: ')

  print '\nUpdating %s...' % GENERAL_SETTINGS_FILE
  with open(GENERAL_SETTINGS_FILE, 'r') as settings_file:
    filedata = settings_file.read()
  filedata = (filedata.replace('\'placeholder_dbm\',',
                               'DBM_SCOPE,' if (uses_dbm == 'Y') else '')
              .replace('\'placeholder_dcm\',',
                       'DCM_SCOPES,' if (uses_dcm == 'Y') else '')
              .replace('\'placeholder_ds\',',
                       'DS_SCOPE,' if (uses_ds == 'Y') else ''))
  with open(GENERAL_SETTINGS_FILE, 'w') as settings_file:
    settings_file.write(filedata)
  print '..done!'

  print '\nUpdating %s...' % PROJECT_SETTINGS_FILE
  with open(PROJECT_SETTINGS_FILE, 'r') as project_file:
    filedata = project_file.read()
  filedata = (filedata.replace('placeholder-project-id', project_id)
              .replace('placeholder-project-root', project_prefix)
              .replace('placeholder-ae-service-account', app_engine_sa)
              .replace('placeholder-gce-service-account', compute_engine_sa)
              .replace('placeholder-dcm-profile-id', dcm_profile_id))
  with open(PROJECT_SETTINGS_FILE, 'w') as project_file:
    project_file.write(filedata)
  print '..done!'

  print '\nUpdating %s...' % QUEUE_FILE
  with open(QUEUE_FILE, 'r') as queue_file:
    filedata = queue_file.read()
  filedata = (filedata.replace('placeholder-user-email', user_email)
              .replace('placeholder-ae-service-account', app_engine_sa)
              .replace('placeholder-ce-service-account', compute_engine_sa))
  with open(QUEUE_FILE, 'w') as queue_file:
    queue_file.write(filedata)
  print '..done!'

  print '\nUpdating %s...' % DEPLOY_FILE
  with open(DEPLOY_FILE, 'r') as deploy_file:
    filedata = deploy_file.read()
  filedata = (filedata.replace('placeholder-python-sources',
                               python_sources_bucket)
              .replace('placeholder-bash-scripts', bash_scripts_bucket)
              .replace('placeholder-project-id', project_id))
  with open(DEPLOY_FILE, 'w') as deploy_file:
    deploy_file.write(filedata)
  print '..done!'

  print '\n\n\n***** SUCCESS!!!'
  print 'Your DNA project has successfully completed the initial setup.'
  print 'You can doublecheck the updated content of the modified files:'
  for filepath in FILES_TO_BACKUP:
    print '- %s' % filepath
  print ('or proceed directly to deploying the files to Google Cloud Platform '
         'running \'%s\'') % DEPLOY_FILE
  print ('\nYou\'ll then need to build your own service, possibly starting from'
         ' the sample service in the \'services\'')
  print ('folder, and to deploy those files too, editing the corresponding '
         'lines in \'%s\', \'cron.yaml\',') % DEPLOY_FILE
  print '\'appengine_config.py\'...'
  print ('\nYou can also run script dna_service_setup.py if you want to create '
         'a service starting from our basic template. See the full '
         'documentation for more info)')
  raw_input('\nThis script has completed, press Enter to exit!')


if __name__ == '__main__':
  main()
