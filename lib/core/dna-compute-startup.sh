#! /bin/bash
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
# Use the metadata server to get the configuration specified during instance creation. Read more
# about metadata here: https://cloud.google.com/compute/docs/metadata#querying
MACHINE_ID=$(curl http://metadata/computeMetadata/v1/instance/attributes/machine-id -H "Metadata-Flavor: Google")
PROJECT_ROOT=$(curl http://metadata/computeMetadata/v1/instance/attributes/project-root -H "Metadata-Flavor: Google")
LEVEL=$(curl http://metadata/computeMetadata/v1/instance/attributes/level -H "Metadata-Flavor: Google")
CE_ENTITY_ID=$(curl http://metadata/computeMetadata/v1/instance/attributes/ce-entity-id -H "Metadata-Flavor: Google")

sudo su
apt-get install sshpass
# Make sure that easy_install is installed
apt-get install python-setuptools

# Create a local folder to copy all python sources to
mkdir $PROJECT_ROOT
cd $PROJECT_ROOT
sudo gsutil -m cp "gs://${PROJECT_ROOT}-python-sources/*" .

# Install required python libraries
sudo easy_install pip
sudo pip install -r requirements.txt

# Run main service
python dna_compute_main.py $LEVEL $CE_ENTITY_ID > stdout.txt 2> stderr.txt
