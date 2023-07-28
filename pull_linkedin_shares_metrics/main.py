import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
from datetime import date
from google.cloud import bigquery
from helpers import (return_active_token,
                     get_from_api,
                     create_bq_table,
                     get_share_list,
                     transform_shares,
                     get_paged_data,
                     filter_no_id)


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

    # Start with first element
    all_shares_start_url = 'https://api.linkedin.com/v2/shares?q=owners&owners=urn:li:organization:30216658&sortBy=LAST_MODIFIED&sharesPerOwner=1000&start=0&count=50'

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Rest.li 2.0 headers
    x_restli_2_0_headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        "X-Restli-Protocol-Version": "2.0.0",
        "Linkedin-Version": "202302"
    }

    # Get all data from API
    all_shares_start_data = get_from_api(all_shares_start_url, headers)
    all_shares_start_data_filtered = filter_no_id(all_shares_start_data['elements'], 'activity')
    all_shares_flattened_data = list(map(transform_shares, all_shares_start_data_filtered))
    all_shares_next_data = get_paged_data(all_shares_start_data, headers)

    # Create base DataFrame with all posts
    df = pd.DataFrame(all_shares_flattened_data)
    df_next = pd.DataFrame(all_shares_next_data)
    df = pd.concat([df, df_next])
    df = df.drop_duplicates(subset=['activity']).reset_index(drop=True)

    # Convert last modified time to datetime format
    df['lastModified_time'] = pd.to_datetime(df['lastModified_time'], unit='ms')

    # Convert created time to datetime format
    df['created_time'] = pd.to_datetime(df['created_time'], unit='ms')

    # Get share IDs from activity IDs
    share_list = pd.Series(get_share_list(df, headers), name='share')

    # Create a list of share metrics
    share_metrics_list = []

    for share in share_list['share']:
        share_url = 'https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658&shares={}'.format(str(share))
        individual_share_data = get_from_api(share_url, headers)
        try:
            individual_share_dict = individual_share_data['elements'][0]['totalShareStatistics']
            individual_share_dict['share'] = individual_share_data['elements'][0]['share']
            share_metrics_list.append(individual_share_dict)
        # Handle cases where no elements attribute not found
        except:
            individual_share_dict = {'uniqueImpressionsCount': 0,
                                     'shareCount': 0,
                                     'engagement': 0.0,
                                     'clickCount': 0,
                                     'likeCount': 0,
                                     'impressionCount': 0,
                                     'commentCount': 0,
                                     'share': str(share)}
            share_metrics_list.append(individual_share_dict)

    share_metrics_df = pd.DataFrame(share_metrics_list)
    share_metrics_df['pull_date'] = date.today()

    # Rename columns
    old_col_names = ['uniqueImpressionsCount',
                     'shareCount',
                     'clickCount',
                     'likeCount',
                     'impressionCount',
                     'commentCount']

    new_col_names = ['unique_impressions_count',
                     'share_count',
                     'click_count',
                     'like_count',
                     'impression_count',
                     'comment_count']

    # Rename cols
    new_cols = dict(zip(old_col_names, new_col_names))
    share_metrics_df = share_metrics_df.rename(columns=new_cols)

    return share_metrics_df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_shares_metrics', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("unique_impressions_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("share_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("engagement", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("click_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("like_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("impression_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("comment_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("share", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pull_date", "DATE", mode="NULLABLE"),
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
