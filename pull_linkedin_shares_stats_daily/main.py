import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
from datetime import date
from google.cloud import bigquery
from helpers import return_active_token, get_from_api, create_bq_table
from flatten_json import flatten
from datetime import date, datetime, timedelta
import calendar


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

    # Calculate time
    end_date = date.today() - timedelta(3)
    start_date = date.today() - timedelta(4)
    midnight_time = datetime.min.time()

    end_datetime = datetime.combine(end_date, midnight_time)
    start_datetime = datetime.combine(start_date, midnight_time)

    time_range_start = calendar.timegm(start_datetime.utctimetuple()) * 1000
    time_range_end = calendar.timegm(end_datetime.utctimetuple()) * 1000

    # Fetch daily data from page statistics API
    url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start={}&timeIntervals.timeRange.end={}".format(time_range_start, time_range_end)

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    data = get_from_api(url, headers)
    endpoint = data['elements']

    # Flatten json
    daily_data_flatten = map(flatten, endpoint)

    # Create dataframe
    df = pd.DataFrame(list(daily_data_flatten))

    # Assign new column names
    old_col_names = ['totalShareStatistics_uniqueImpressionsCount',
                     'totalShareStatistics_shareCount',
                     'totalShareStatistics_engagement',
                     'totalShareStatistics_clickCount',
                     'totalShareStatistics_likeCount',
                     'totalShareStatistics_impressionCount',
                     'totalShareStatistics_commentCount',
                     'organizationalEntity',
                     'timeRange_start',
                     'timeRange_end']

    new_col_names = ['unique_impressions_count',
                     'share_count',
                     'engagement',
                     'click_count',
                     'like_count',
                     'impression_count',
                     'comment_count',
                     'organizational_entity',
                     'time_range_start',
                     'time_range_end']

    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    # Write dataframe to csv
    df.to_csv('linkedin_share_stats_daily.csv', encoding='utf-8')

    return "Data has been saved"

    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_share_stats_daily', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("unique_impressions_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("share_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("engagement", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("click_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("like_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("impression_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("comment_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("organizational_entity", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("time_range_start", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("time_range_end", "DATE", mode="REQUIRED")
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
