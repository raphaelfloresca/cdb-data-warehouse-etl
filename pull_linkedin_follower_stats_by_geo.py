import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URL for page stats and
    follower_stats_url = 'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658'
    geo_url = 'https://api.linkedin.com/v2/geo?ids=List('

    headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
        'Linkedin-Version': '202302'
    }


    x_restli_2_0_headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
        "X-Restli-Protocol-Version": "2.0.0",
        "Linkedin-Version": "202302"
    }

    # Get all data from API
    follower_stats_data = get_from_api(follower_stats_url, headers)
    endpoint = follower_stats_data['elements'][0]

    # Fetch geo data only
    geo_data = endpoint['followerCountsByGeo']

    # Convert into a dataframe
    df = pd.json_normalize(geo_data)

    # Geo index
    geo_index = get_from_geo_api(geo_url, x_restli_2_0_headers, df['geo'])
    # print(geo_index)

    # Initialize geo list
    geo_list = []

    # Replace URI with readable name
    for geo in geo_data:
        stripped_geo = geo['geo'].lstrip('urn:li:geo:')
        geo_list.append(geo_uri_to_en(geo_index, stripped_geo))

    # Append to dataframe
    df["geo"] = pd.Series(geo_list)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['geo',
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
    df.to_csv('linkedin_follower_stats_geo.csv', encoding='utf-8')

    return "Data has been saved"
