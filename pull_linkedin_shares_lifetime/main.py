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

    # Fetch daily data from organization share API
    lifetime_share_url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658"

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Pull data
    lifetime_shares_data = get_from_api(lifetime_share_url, headers, 'elements')
    df = pd.json_normalize(lifetime_shares_data)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['pull_date',
            'totalShareStatistics.uniqueImpressionsCount',
            'totalShareStatistics.shareCount',
            'totalShareStatistics.shareMentionsCount',
            'totalShareStatistics.engagement',
            'totalShareStatistics.clickCount',
            'totalShareStatistics.likeCount',
            'totalShareStatistics.impressionCount',
            'totalShareStatistics.commentMentionsCount',
            'totalShareStatistics.commentCount']
    df = df[cols]

    # Rename columns
    old_col_names = ['totalShareStatistics.uniqueImpressionsCount',
                     'totalShareStatistics.shareCount',
                     'totalShareStatistics.shareMentionsCount',
                     'totalShareStatistics.engagement',
                     'totalShareStatistics.clickCount',
                     'totalShareStatistics.likeCount',
                     'totalShareStatistics.impressionCount',
                     'totalShareStatistics.commentMentionsCount',
                     'totalShareStatistics.commentCount']

    new_col_names = ['unique_impressions_count',
                     'share_count',
                     'share_mentions_count',
                     'engagement',
                     'click_count',
                     'like_count',
                     'impression_count',
                     'comment_mentions_count',
                     'comment_count']

    # Assign new column names
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_shares_lifetime', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("pull_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("unique_impressions_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("share_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("share_mentions_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("engagement", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("click_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("like_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("impression_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("comment_mentions_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("comment_count", "INTEGER", mode="NULLABLE"),
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
