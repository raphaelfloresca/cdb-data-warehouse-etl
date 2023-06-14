import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URLs for follower stats and functions
    follower_stats_url = 'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658'
    function_url = 'https://api.linkedin.com/v2/functions/'

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    follower_stats_data = get_from_api(follower_stats_url, headers)
    endpoint = follower_stats_data['elements'][0]
    function_index = get_from_api(function_url, headers)

    # Fetch function data only
    function_data = endpoint['followerCountsByFunction']

    # Replace URI with readable name
    for function in function_data:
        function['function'] = function_uri_to_en(function_index, function['function'])

    # Convert into a dataframe
    df = pd.json_normalize(function_data)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['function',
            'pull_date',
            'followerCounts.organicFollowerCount',
            'followerCounts.paidFollowerCount']
    df = df[cols]

    # Rename columns
    old_col_names = ['followerCounts.organicFollowerCount',
                     'followerCounts.paidFollowerCount']

    new_col_names = ['organic_follower_count',
                     'paid_follower_count']

    # Assign new column names
    new_cols = dict(zip(old_col_names, new_col_names))

    df = df.rename(columns=new_cols)

    # Write dataframe to csv
    df.to_csv('linkedin_follower_stats_function.csv', encoding='utf-8')

    return "Data has been saved"
