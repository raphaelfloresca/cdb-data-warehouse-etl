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

    # URL for follower count
    endpoint = 'urn:li:organization:30216658?edgeType=CompanyFollowedByMember'
    api_url = 'https://api.linkedin.com/rest/networkSizes/{}'.format(endpoint)

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    follower_count_data = get_from_api(api_url, headers)

    # Initialize dataframe
    df = pd.DataFrame(follower_count_data, index=[0])

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['pull_date',
            'firstDegreeSize']
    df = df[cols]

    # Rename columns
    old_col_names = ['firstDegreeSize']
    new_col_names = ['follower_count']
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_follower_count', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("pull_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("follower_count", "INTEGER", mode="REQUIRED"),
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
