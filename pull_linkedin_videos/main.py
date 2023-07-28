import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
from datetime import date
from google.cloud import bigquery
from dotenv import load_dotenv
from helpers import (
    return_active_token,
    get_from_api,
    create_bq_table,
    transform_ugc,
    get_paged_ugc_data)


'''
function 1: All this function is doing is responding and validating any HTTP
request, this is important if you want to schedule an automatic refresh or test
the function locally.
'''


def validate_http(request):

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and request_args:
        pull_to_prod()
        return f'Data pull complete'
    elif request_json and request_args:
        pull_to_prod()
        return f'Data pull complete'
    else:
        pull_to_prod()
        return f'Data pull complete'


'''
function 2: This is the code to pull data from the API and store it in a DataFrame
'''


def get_api_data():

    # Pull start data
    ugc_endpoint = "https://api.linkedin.com/v2/ugcPosts?q=authors&count=10&sortBy=LAST_MODIFIED&authors=List(urn%3Ali%3Aorganization%3A30216658)"
    api_headers_2_0 = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        "LinkedIn-Version": "202302",
        "X-Restli-Protocol-Version": "2.0.0"
    }
    ugc_request_data = get_from_api(ugc_endpoint, headers=api_headers_2_0)

    # Get paged data for UGC posts
    paged_data = get_paged_ugc_data(ugc_request_data, api_headers_2_0)

    # Pull only videos into video list
    video_list = []
    for page in paged_data:
        for row in page['elements']:
            if row['specificContent']['com.linkedin.ugc.ShareContent']['shareMediaCategory'] == 'VIDEO':
                row = transform_ugc(row)
                video_list.append(row)

    df = pd.DataFrame(video_list)
    df['publish_time'] = pd.to_datetime(df['publish_time'], unit='ms')

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_videos', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publish_time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    ]

    table = create_bq_table(schema)
    df = get_api_data()
    bq_load(table, df, 'marketing_staging')

    return "Data has been loaded to BigQuery"


'''
function 5: This pulls the data to a csv - used for testing
'''


def pull_to_csv():

    df = get_api_data()
    df.to_csv("test.csv")

    return "Data has been saved"


'''
function 6: This function just converts your pandas dataframe into a bigquery
table, you'll also need to designate the name and location of the table in the
variable names below.
'''


def bq_load(key, value, dataset_name):

    project_name = 'marketing-bd-379302'
    table_name = key

    value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name),
                 project_id=project_name, if_exists='append')
