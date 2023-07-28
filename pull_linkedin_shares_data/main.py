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
                     split,
                     get_chunked_social_metadata,
                     get_all_social_metadata,
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
    share_list = get_share_list(df, headers)
    activity_to_share_df = pd.concat([pd.Series(share_list, name='share'), df['activity']], axis=1)

    # Append share IDs to DataFrame
    df_with_share_id = df.join(
        activity_to_share_df.set_index(["activity"]),
        on=["activity"],
        how="inner",
        lsuffix="_x",
        rsuffix="_y"
    )

    # Split share list into chunks of 50
    split_share_list = list(split(share_list, 50))

    # Get all social metadata
    chunked_social_metadata = get_chunked_social_metadata(split_share_list, x_restli_2_0_headers)
    social_metadata_list = get_all_social_metadata(chunked_social_metadata)
    social_metadata_df = pd.DataFrame(social_metadata_list)

    # Append all social metadata to DataFrame
    df_with_social_metadata = df_with_share_id.join(
        social_metadata_df.set_index(["share"]),
        on=["share"],
        how="inner",
        lsuffix="_x",
        rsuffix="_y"
    )

    # All data pulled associated with the date of the API request - allows for partitioning laterr
    df_with_social_metadata["pull_date"] = pd.to_datetime(date.today())

    # Reorder and rername columns
    cols = ['activity',
            'share',
            'pull_date',
            'created_time',
            'lastModified_time',
            'created_actor',
            'text',
            'like_count',
            'praise_count',
            'maybe_count',
            'empathy_count',
            'interest_count',
            'appreciation_count',
            'comments_state',
            'comments_count',
            'comments_top_level_count']

    df_with_social_metadata = df_with_social_metadata[cols]
    df_with_social_metadata = df_with_social_metadata.rename(columns={'lastModified_time': 'last_modified_time'})

    return df_with_social_metadata


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_social_metadata_shares', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("activity", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("share", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pull_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("created_time", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("last_modified_time", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("created_actor", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("like_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("praise_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("maybe_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("empathy_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("interest_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("appreciation_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("comments_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comments_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("comments_top_level_count", "INTEGER", mode="NULLABLE")
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
