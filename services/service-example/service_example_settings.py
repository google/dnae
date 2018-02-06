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

"""DNA - Service example - Settings constants."""

from dna_project_settings import GCS_PROJECT_ROOT
from utils import SelfIncreasingIndex

SERVICE_NAME = "SERVICE-EXAMPLE"

# BigQuery Dataset and table names.
GBQ_DATASET = "service_example_dataset"
GBQ_TABLE = "dcm_data"
# Cloud Storage bucket name.
GCS_BUCKET = "%s-service-example" % GCS_PROJECT_ROOT
# Main script to run on the Compute Engine instance.
GCE_RUN_SCRIPT = "./service-example-run.sh"

# DCM report default values.
DCM_REPORT_NAME = "ServiceExampleReport"
DCM_REPORT_DATE_RANGE = "YESTERDAY"

INDEX = SelfIncreasingIndex()

# DCM Report template structure.
DCM_REPORT_TEMPLATE = {
    "name": "<PLACEHOLDER>",
    "type": "STANDARD",
    "fileName": "<PLACEHOLDER>",
    "format": "CSV",
    "criteria": {
        "dateRange": {
            "relativeDateRange": "<PLACEHOLDER>"
        },
        "dimensions": [{
            "name": "dfa:advertiser"
        }, {
            "name": "dfa:advertiserId"
        }, {
            "name": "dfa:campaign"
        }, {
            "name": "dfa:campaignId"
        }, {
            "name": "dfa:placementSize"
        }, {
            "name": "dfa:creativeType"
        }, {
            "name": "dfa:creativeSize"
        }, {
            "name": "dfa:platformType"
        }, {
            "name": "dfa:site"
        }, {
            "name": "dfa:month"
        }, {
            "name": "dfa:week"
        }, {
            "name": "dfa:date"
        }],
        "metricNames": [
            "dfa:clicks",
            "dfa:impressions",
            "dfa:activeViewAverageViewableTimeSecond",
            "dfa:activeViewEligibleImpressions",
            "dfa:activeViewMeasurableImpressions",
            "dfa:activeViewViewableImpressions",
        ]
    }
}

# Field map structure.
FIELD_MAP_STANDARD = {
    "Advertiser": {
        "idx": INDEX.start()
    },
    "AdvertiserID": {
        "idx": INDEX.nextval()
    },
    "Campaign": {
        "idx": INDEX.nextval()
    },
    "CampaignID": {
        "idx": INDEX.nextval()
    },
    "PlacementSize": {
        "idx": INDEX.nextval()
    },
    "CreativeType": {
        "idx": INDEX.nextval()
    },
    "CreativeSize": {
        "idx": INDEX.nextval()
    },
    "PlatformType": {
        "idx": INDEX.nextval()
    },
    "Site": {
        "idx": INDEX.nextval()
    },
    "Month": {
        "idx": INDEX.nextval()
    },
    "Week": {
        "idx": INDEX.nextval()
    },
    "Date": {
        "idx": INDEX.nextval()
    },
    "Clicks": {
        "idx": INDEX.nextval()
    },
    "Impressions": {
        "idx": INDEX.nextval()
    },
    "ViewableTimeSeconds": {
        "idx": INDEX.nextval()
    },
    "EligibleImpressions": {
        "idx": INDEX.nextval()
    },
    "MeasurableImpressions": {
        "idx": INDEX.nextval()
    },
    "ViewableImpressions": {
        "idx": INDEX.nextval()
    },
}

# Table Data schema
DATA_SCHEMA_STANDARD = {
    "fields": [
        {
            "name": "Advertiser",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "AdvertiserID",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Campaign",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "CampaignID",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "PlacementSize",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "CreativeType",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "CreativeSize",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "PlatformType",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Site",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Month",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Week",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Date",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "Clicks",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "Impressions",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "ViewableTimeSeconds",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },
        {
            "name": "EligibleImpressions",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "MeasurableImpressions",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
        {
            "name": "ViewableImpressions",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },
    ]
}
