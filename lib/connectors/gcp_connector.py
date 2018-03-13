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

"""Connector modules - Connector for GCP.

This module provides access to Google Cloud Platforms's data (DataStore,
CloudStorage, BigQuery)
"""
import csv
import io
import logging
import time
import uuid

from api_handlers import APIRequest
from gcloud import bigquery
from gcloud import datastore
from gcloud import storage
from googleapiclient import discovery
from googleapiclient import http as httpMediaHandler
from oauth2client.client import GoogleCredentials
from utils import retry


class GCPTable(object):
  """Representation of a BigQuery table."""

  def __init__(self, schema, encoding='UTF-8'):
    self.__table = list()
    self.schema = schema
    self.encoding = encoding

  def getfields(self):
    fields = list()
    schema_fields = self.schema.get('fields')
    for field in schema_fields:
      fields.append(field.get('name'))
    return fields

  def ingest(self, data, force_schema=False, skip_headers=True):
    """Add several rows to table.

    Args:
      data: list of rows
      force_schema: Whether schema should be enforced or not
      skip_headers: Whether first line should be skipped or not
    """
    start = 0
    if skip_headers:
      start = 1
    for row in data[start:]:
      self.addrow(row, force_schema)

  def addrow(self, row, force_schema=False):
    """Add row to table.

    Args:
      row: Row to be added
      force_schema: If True, each field is casted to the type in the schema
    """
    assert isinstance(row, list)
    assert self.schema
    schema_fields = self.schema.get('fields')
    assert len(row) == len(schema_fields)

    idx = 0
    for field in schema_fields:
      field_type = field.get('type')

      if (field_type == 'STRING') and not isinstance(row[idx], str):
        if force_schema:
          row[idx] = str(row[idx])
        else:
          logging.warning('Field type mismatch. '
                          'Type "str" expected for field %s '
                          'at position %s', idx, field.get('name'))

      if (field_type == 'FLOAT') and not isinstance(row[idx], float):
        if force_schema:
          value = row[idx]
          if not value:
            value = 0.0
          row[idx] = float(value)
        else:
          logging.warning('Field type mismatch. '
                          'Type "float" expected for field %s '
                          'at position %s', idx, field.get('name'))

      if (field_type == 'INTEGER') and not isinstance(row[idx], int):
        if force_schema:
          value = row[idx]
          if not value:
            value = 0
          row[idx] = int(value)
        else:
          logging.warning('Field type mismatch. '
                          'Type "int" expected for field %s '
                          'at position %s', idx, field.get('name'))

      if (field_type == 'BOOLEAN') and not isinstance(row[idx], bool):
        if force_schema:
          row[idx] = bool(row[idx])
        else:
          logging.warning('Field type mismatch. '
                          'Type "bool" expected for field %s '
                          'at position %s', idx, field.get('name'))
      idx += 1

    self.__table.append(row)

  def dumptofile(self, filename):
    """Writes table to file.

    Writes this table to file. Format is CSV.

    Args:
      filename: Name of the output file
    """
    csv_file = open(filename, 'wb')
    writer = csv.writer(csv_file)
    for row in self.__torows():
      writer.writerow(row)
    csv_file.close()

  def tostrrows(self):
    """Return this table as a list of CSV rows.

    Returns:
      List of strings. Each element is a row, with values separated by commas
    """
    strrows = list()
    rows = self.__torows()
    for row in rows:
      strrows.append(','.join(map(str, row)))
    return strrows

  def __torows(self):
    assert self.schema
    column_names = list()
    for field in self.schema.get('fields'):
      column_names.append(field.get('name'))
    rows = list()
    rows.append(column_names)
    for row in self.__table:
      rows.append(row)
    return rows


class GCPConnector(object):
  """GCP Connector class.

  This class provides methods to access Google Cloud APIs and storing/reading
  data to/from DataStore, CloudStorage and BigQuery
  """
  _gcs_apiver = 'v1'
  _gbq_apiver = 'v2'
  _gce_apiver = 'v1'
  _gds_apiver = 'v1'
  _gtq_apiver = 'v1beta2'
  _gct_apiver = 'v2beta2'

  @retry
  def __init__(self, project_id):

    credentials = GoogleCredentials.get_application_default()

    self.project_id = project_id
    self.gce_zone = None
    # Change the following location if your GAE instance is not in US-CENTRAL
    # See GAE locations at: https://cloud.google.com/appengine/docs/locations
    self.gae_location = 'us-central1'
    self.__gcsapi = discovery.build(
        'storage', self._gcs_apiver, credentials=credentials)
    self.__gbqapi = discovery.build(
        'bigquery', self._gbq_apiver, credentials=credentials)
    self.__gceapi = discovery.build(
        'compute', self._gce_apiver, credentials=credentials)
    self.__gdsapi = discovery.build(
        'datastore', self._gds_apiver, credentials=credentials)
    self.__gtqapi = discovery.build(
        'taskqueue', self._gtq_apiver, credentials=credentials)
    self.__gctapi = discovery.build(
        'cloudtasks', self._gct_apiver, credentials=credentials)

    self.__gcsclient = storage.Client(project_id)
    self.__gdsclient = datastore.Client(project_id)
    self.__gbqclient = bigquery.Client(project_id)

  # Cloud Storage methods
  @retry
  def gcs_uploadtable(self, gcptable, bucket, filename):
    """Upload table to Cloud Storage as a CSV.

    Args:
      gcptable: GCPTable instance
      bucket: Target Cloud Storage bucket
      filename: Target filename insice the Cloud Storage bucket
    Returns:
      URI of the newly created file
    """
    assert isinstance(gcptable, GCPTable)
    fh = io.BytesIO()
    for row in gcptable.tostrrows():
      fh.writelines(row + '\n')
    media = httpMediaHandler.MediaIoBaseUpload(fh, 'text/csv', resumable=True)
    request = self.__gcsapi.objects().insert(
        bucket=bucket, name=filename, media_body=media)
    APIRequest(request).execute()
    gcsuri = 'gs://{bucket}/{name}'.format(bucket=bucket, name=filename)
    return gcsuri

  def gcs_getblobs(self, bucket_name):
    bucket = self.__gcsclient.get_bucket(bucket_name)
    return bucket.list_blobs()

  def gcs_getapiresource(self):
    return self.__gcsapi

  def gcs_getclient(self):
    return self.__gcsclient

  # Google BigQuery methods

  def bq_importfromgcs(self,
                       gcsuri,
                       dataset,
                       table,
                       schema,
                       encoding,
                       writemode='WRITE_TRUNCATE'):
    """Import CSV in CloudStorage into BigQuery table.

    Args:
      gcsuri: URI of the CloudStorage file
      dataset: Target BigQuery dataset
      table: Target BigQuery table
      schema: Schema for the new BigQuery table
      encoding: Encoding of the file in CloudStorage
      writemode: Write mode for the new table
    Returns:
      BigQuery's import job ID
    """
    if isinstance(gcsuri, list):
      source_uris = gcsuri
    else:
      source_uris = [gcsuri]

    job_id = str(uuid.uuid4())
    job_data = {
        'jobReference': {
            'projectId': self.project_id,
            'jobId': job_id
        },
        'configuration': {
            'load': {
                'sourceUris': source_uris,
                'schema': schema,
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': dataset,
                    'tableId': table
                },
                'skipLeadingRows': 1,
                'writeDisposition': writemode,
                'fieldDelimiter': ',',
                'encoding': encoding,
                'allowLargeResults': True
            }
        }
    }
    request = self.__gbqapi.jobs().insert(
        projectId=self.project_id, body=job_data)
    APIRequest(request).execute()
    return job_id

  def bq_copytable(self,
                   source_dataset,
                   source_table,
                   dest_dataset,
                   dest_table,
                   writemode='WRITE_TRUNCATE'):
    """Copy table from one BigQuery dataset to another.

    Args:
      source_dataset: Source BigQuery dataset
      source_table: Source BigQuery table
      dest_dataset: Destination BigQuery dataset
      dest_table: Destination BigQuery table
      writemode: Write mode for the destination BigQuery table
    Returns:
      BigQuery's job ID
    """
    job_id = str(uuid.uuid4())
    job_data = {
        'jobReference': {
            'projectId': self.project_id,
            'jobId': job_id
        },
        'configuration': {
            'copy': {
                'sourceTable': {
                    'projectId': self.project_id,
                    'datasetId': source_dataset,
                    'tableId': source_table
                },
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': dest_dataset,
                    'tableId': dest_table
                },
                'writeDisposition': writemode,
            }
        }
    }
    request = self.__gbqapi.jobs().insert(
        projectId=self.project_id, body=job_data)
    APIRequest(request).execute()
    return job_id

  def bq_getjobinfo(self, job_id):
    request = self.__gbqapi.jobs().get(projectId=self.project_id, jobId=job_id)
    res = APIRequest(request).execute()
    return res

  def bq_waitjob(self, job_id):
    job_is_running = True
    while job_is_running:
      job_info = self.bq_getjobinfo(job_id)
      job_status = job_info['status']['state']
      if job_status != 'RUNNING':
        job_is_running = False
    return job_status

  def bq_readtable(self, dataset, table):
    """Read BigQuery table and return data.

    Args:
      dataset: Dataset ID
      table: Table ID
    Returns:
      Table data. Returned value is a list. Each element is a row. Each row
        is a list containing as many elements as columns.
    """
    request = self.__gbqapi.tabledata().list(
        projectId=self.project_id, datasetId=dataset, tableId=table)
    result = APIRequest(request).execute()
    data = list()
    if 'rows' in result:
      for row in result['rows']:
        values = list()
        for item in row['f']:
          values.append(item['v'])
        data.append(values)
    return data

  def bq_insertdata(self, gcptable, dataset, table):
    """Insert GCPTable data into BigQuery table.

    Args:
      gcptable: GCPTable instance
      dataset: BigQuery dataset ID
      table: BigQuery table ID
    Returns:
      bigquery#tableDataInsertAllResponse resource (see
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response)
    """
    assert isinstance(gcptable, GCPTable)
    body = {'skipInvalidRows': True, 'ignoreUnknownValues': True, 'rows': []}
    fields = gcptable.getfields()
    for row in gcptable.table:  # Access raw data
      json_row = dict()
      for i in range(len(fields)):
        json_row[fields[i]] = row[i]
      body['rows'].append({'json': json_row})
    request = self.__gbqapi.tabledata().insertAll(
        projectId=self.project_id, datasetId=dataset, tableId=table, body=body)
    result = APIRequest(request).execute()
    return result

  def bq_query(self, dataset, query, format_as_dict=None):
    """Execute query on BigQuery.

    Args:
      dataset: BigQuery dataset ID
      query: Query to execute
      format_as_dict: Whether output should be returned as a dict or not
    Returns:
      Query result. If format_as_dict, it returns a dict. This dict contains
        one key per value of the first field (as many keys as rows in the
        resultset). For each key, the value is another dict that contains
        the values, for that row, for every remaining field, indexed by
        the field name, which acts as key.
        In the case of format_as_dict=False, the result is a list of lists. The
        outer list are rows and the inner list are columns
    """
    body = {'query': query, 'defaultDataset': {'datasetId': dataset}}
    request = self.__gbqapi.jobs().query(projectId=self.project_id, body=body)
    result = APIRequest(request).execute()
    fields = result['schema']['fields']
    list_data = list()
    dict_data = dict()
    if 'rows' in result:
      headers = list()
      for f in fields:
        headers.append(f['name'])
      list_data.append(headers)
      for record in result['rows']:
        row = list()
        for idx in range(len(fields)):
          row.append(record['f'][idx]['v'])
          if idx == 0:
            dict_data[record['f'][0][
                'v']] = dict()  # Assume first field of the query is the key
          else:
            dict_data[record['f'][0]['v']][fields[idx]['name']] = (
                record['f'][idx]['v'])
        list_data.append(row)
    if format_as_dict:
      return dict_data
    else:
      return list_data

  def bq_inserttable(self, dataset, table, schema):
    """Create new BigQuery table.

    Args:
      dataset: BigQuery ataset ID
      table: BigQuery table ID
      schema: Schema descriptor
    """
    body = {
        'tableReference': {
            'projectId': self.project_id,
            'datasetId': dataset,
            'tableId': table
        },
        'schema': schema,
    }
    request = self.__gbqapi.tables().insert(
        projectId=self.project_id, datasetId=dataset, body=body)
    APIRequest(request).execute()

  def bq_deletetable(self, dataset, table):
    """Delete bigQuery table.

    Args:
      dataset: BigQuery dataset ID
      table: BigQuery table ID
    """
    request = self.__gbqapi.tables().delete(
        projectId=self.project_id, datasetId=dataset, tableId=table)
    APIRequest(request).execute()

  def bq_gettables(self, dataset):
    """Get BigQuery tables for a given dataset.

    Args:
      dataset: BigQuery dataset ID
    Returns:
      List of bigquery#table resources. See (
      https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource-representations)
    """
    table_list = list()
    tables = self.__gbqapi.tables()
    request = tables.list(projectId=self.project_id, datasetId=dataset)
    while request is not None:
      response = APIRequest(request).execute()
      if (response['totalItems'] > 0) and ('tables' in response):
        for table in response['tables']:
          table_list.append(table)
      request = tables.list_next(
          previous_request=request, previous_response=response)
    return table_list

  def bq_existtable(self, dataset, table):
    """Check if BigQuery table exists.

    Args:
      dataset: BigQuery dataset ID
      table: BigQuery table ID
    Returns:
      True if table exists, False otherwise
    """
    table_list = self.bq_gettables(dataset)
    for item in table_list:
      if item['tableReference']['tableId'] == table:
        return True
    return False

  def bq_getapiresource(self):
    return self.__gbqapi

  def bq_getclient(self):
    return self.__gbqclient

  # Google Compute Engine methods

  def gce_setzone(self, zone='europe-west1-b'):
    self.gce_zone = zone

  def gce_configinstance(self, name, zone, machine_type, service_account,
                         scopes):
    """Create configuration for GCE instance.

    Args:
      name: Instance name
      zone: zone (e.g.: us-central1-f)
      machine_type: Machine type (e.g.: n1-standard-1)
      service_account: email of service account authorized for this instance
      scopes: Authorization scopes for service account
    Returns:
      Dict object with instance resource (see
        https://cloud.google.com/compute/docs/reference/latest/instances#resource)
    """
    self.gce_zone = zone

    request = self.__gceapi.images().getFromFamily(
        project='debian-cloud', family='debian-8')
    image_response = APIRequest(request).execute()
    source_disk_image = image_response['selfLink']

    config = {
        'name':
            name,
        'description':
            'Auto created instance',
        'machineType':
            'zones/%s/machineTypes/%s' % (self.gce_zone, machine_type),
        'networkInterfaces': [{
            'network':
                'global/networks/default',
            'accessConfigs': [{
                'type': 'ONE_TO_ONE_NAT',
                'name': 'External NAT'
            }]
        }],
        'disks': [{
            'boot': True,
            'autoDelete': True,
            'initializeParams': {
                'sourceImage': source_disk_image,
            }
        }],
        'metadata': {
            'items': []
        },
        'serviceAccounts': [{
            'email': service_account,
            'scopes': scopes
        }]
    }

    return config

  def gce_addmetadata(self, config, key, value):
    config['metadata']['items'].append({'key': key, 'value': value})
    return config

  def gce_createinstance(self, config):
    request = self.__gceapi.instances().insert(
        project=self.project_id, zone=self.gce_zone, body=config)
    return APIRequest(request).execute()

  def gce_listinstances(self, zone):
    request = self.__gceapi.instances().list(project=self.project_id, zone=zone)
    return APIRequest(request).execute()

  def gce_deleteinstance(self, name, zone=None):
    if not zone:
      assert self.gce_zone
    else:
      self.gce_zone = zone
    request = self.__gceapi.instances().delete(
        project=self.project_id, zone=self.gce_zone, instance=name)
    return APIRequest(request).execute()

  def gce_waitforoperation(self, opname):
    assert self.gce_zone
    retries_left = 120
    while (retries_left > 0):
      request = self.__gceapi.zoneOperations().get(
          project=self.project_id, zone=self.gce_zone, operation=opname)
      result = APIRequest(request).execute()
      if result['status'] == 'DONE':
        if 'error' in result:
          raise Exception(result['error'])
        return result
      time.sleep(1)
      retries_left -= 1
    raise Exception('Timeout')

  # Google Datastore methods

  @retry
  def gds_insert(self, kind, attributes):
    with self.__gdsclient.transaction():
      incomplete_key = self.__gdsclient.key(kind)
      entity = datastore.Entity(key=incomplete_key)
      entity.update(attributes)
      self.__gdsclient.put(entity)
    return entity

  @retry
  def gds_update(self, entity):
    with self.__gdsclient.transaction():
      self.__gdsclient.put(entity)
    return entity

  @retry
  def gds_delete(self, entity_or_key):
    if isinstance(entity_or_key, datastore.entity.Entity):
      self.__gdsclient.delete(entity_or_key.key)
    elif isinstance(entity_or_key, datastore.key.Key):
      self.__gdsclient.delete(entity_or_key)
    return 0

  @retry
  def gds_query(self, kind, sort=None):
    query = self.__gdsclient.query(kind=kind)
    if sort == 'asc':
      query.order = ['created']
    elif sort == 'desc':
      query.order = ['-created']
    return list(query.fetch())

  @retry
  def gds_querybykey(self, kind, key):
    query = self.__gdsclient.query(kind=kind)
    query.key_filter(key=key)
    return list(query.fetch())

  @retry
  def gds_querybyid(self, kind, entity_id):
    query = self.__gdsclient.query(kind=kind)
    item_list = list(query.fetch())
    for item in item_list:
      if item.key.id == entity_id:
        return item
    return None

  def gds_getclient(self):
    return self.__gdsclient

  def gds_getapiresource(self):
    return self.__gdsapi

  # App Engine Task Queue
  def gtq_inserttask(self,
                     project_id,
                     queue_name,
                     payload,
                     max_leases=None,
                     tag=None):
    """Insert new task into AppEngine task queue.

    Args:
      project_id: Google Cloud project ID
      queue_name: Taks queue name
      payload: Task payload
      max_leases: Maximum number of leases
      tag: Tag for the task
    Returns:
      Task resource for the newly created task (see
      https://cloud.google.com/appengine/docs/standard/python/taskqueue/rest/tasks#resource)
    """
    body = {
        'queueName': queue_name,
        'payloadBase64': payload,
        'leaseTimestamp': long(time.time() * 1e6)
    }
    if max_leases:
      body['retry_count'] = max_leases
    if tag:
      body['tag'] = tag

    request = self.__gtqapi.tasks().insert(
        project=project_id, taskqueue=queue_name, body=body)
    response = APIRequest(request).execute()
    return response

  def gtq_leasetask(self, queue_name, lease_secs, num_tasks=1):
    request = self.__gtqapi.tasks().lease(
        project=self.project_id,
        taskqueue=queue_name,
        leaseSecs=lease_secs,
        numTasks=num_tasks)
    response = APIRequest(request).execute()
    return response

  def gtq_gettask(self, queue_name, task_id):
    request = self.__gtqapi.tasks().get(
        project=self.project_id, taskqueue=queue_name, task=task_id)
    response = APIRequest(request).execute()
    return response

  def gtq_listtasks(self, queue_name):
    request = self.__gtqapi.tasks().list(
        project=self.project_id, taskqueue=queue_name)
    response = APIRequest(request).execute()
    return response

  def gtq_deletetask(self, project_id, queue_name, task_id):
    request = self.__gtqapi.tasks().delete(
        project=project_id, taskqueue=queue_name, task=task_id)
    APIRequest(request).execute()

  def gtq_gettaskqueue(self, queue_name):
    request = self.__gtqapi.taskqueues().get(
        project='s~' + self.project_id, taskqueue=queue_name, getStats=True)
    response = APIRequest(request).execute()
    return response

  # Cloud Tasks
  def gct_acknowledgetask(self, task_name, schedule_time):
    ack_body = {
        'scheduleTime': schedule_time
    }
    request = self.__gctapi.projects().locations().queues().tasks().acknowledge(
        name=task_name, body=ack_body)
    response = APIRequest(request).execute()
    return response

  def gct_gettask(self, task_name):
    request = self.__gctapi.projects().locations().queues().tasks().get(
        name=task_name, responseView='FULL')
    response = APIRequest(request).execute()
    return response

  def gct_createtask(self, queue_name, payload, tag=None):
    """Insert new task into the Cloud Task queue.

    Args:
      queue_name: Cloud Task queue name
      payload: Task payload
      tag: Tag for the task
    Returns:
      Task resource for the newly created task
    """
    parent = ('projects/' + self.project_id +  '/locations/' +
              self.gae_location + '/queues/' + queue_name)
    body = {
        'responseView': 'FULL',
        'task': {
            'pullMessage': {
                'payload': payload
            }
        }
    }
    if tag:
      body['task']['pullMessage']['tag'] = tag

    request = self.__gctapi.projects().locations().queues().tasks().create(
        parent=parent, body=body)
    response = APIRequest(request).execute()
    return response

  def gct_listtasks(self, queue_name):
    parent = ('projects/' + self.project_id +  '/locations/' +
              self.gae_location + '/queues/' + queue_name)
    request = self.__gctapi.projects().locations().queues().tasks().list(
        parent=parent)
    response = APIRequest(request).execute()
    return response

  def gct_leasetask(self, queue_name, lease_secs, num_tasks=1):
    """Lease first available task(s) from the specified Cloud Task queue.

    Args:
      queue_name: Cloud Task queue name
      lease_secs: Duration of the lease in seconds
      num_tasks: Number of tasks to lease
    Returns:
      Resource of the leased task(s)
    """
    parent = ('projects/' + self.project_id +  '/locations/' +
              self.gae_location + '/queues/' + queue_name)
    duration = str(lease_secs) + 's'
    lease_body = {
        'maxTasks': num_tasks,
        'responseView': 'FULL',
        'leaseDuration': duration
    }
    request = self.__gctapi.projects().locations().queues().tasks().lease(
        parent=parent, body=lease_body)
    response = APIRequest(request).execute()
    return response
