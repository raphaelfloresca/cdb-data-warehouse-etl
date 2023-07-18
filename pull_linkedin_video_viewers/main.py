import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
import time
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
                row['video'] = row['id']
                row['publish_time'] = row['firstPublishedAt']
                video_list.append(row)

    current_time = int(time.time() * 1000)

    df = pd.DataFrame()

    for video in video_list:
        endpoint = "https://api.linkedin.com/rest/videoAnalytics?q=entity&entity={}&type=VIEWER&aggregation=DAY&timeRange.start={}&timeRange.end={}".format(str(video['video']), str(video['publish_time']), str(current_time))
        api_headers = {
            "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
            "LinkedIn-Version": "202302"
        }
        video_analytics_request = requests.get(endpoint, headers=api_headers)
        video_analytics_data = video_analytics_request.json()
        df_single = pd.json_normalize(video_analytics_data['elements'])
        df_single['timeRange.start'] = pd.to_datetime(df_single['timeRange.start'], unit='ms')
        df_single['timeRange.end'] = pd.to_datetime(df_single['timeRange.end'], unit='ms')
        df_single.rename(columns={'statisticsType': 'statistics_type',
                           'timeRange.start': 'time_range_start',
                           'timeRange.end': 'time_range_end'},
                  inplace=True)
        df = pd.concat([df, df_single], ignore_index=True)

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_follower_count', df, 'marketing')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("statistics_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("entity", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("time_range_start", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("time_range_end", "DATE", mode="REQUIRED"),
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
