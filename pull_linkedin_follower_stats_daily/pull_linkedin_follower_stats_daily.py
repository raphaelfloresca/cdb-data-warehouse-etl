import pandas as pd
from flatten_json import flatten
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Fetch daily data from follower statistics API
    url = "https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:2414183&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start=1679911200000&timeIntervals.timeRange.end=1681812000000"

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
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
    old_col_names = ['followerGains_organicFollowerGain',
                     'followerGains_paidFollowerGain',
                     'organizationalEntity',
                     'timeRange_start',
                     'timeRange_end']

    new_col_names = ['organic_follower_gain',
                     'paid_follower_gain',
                     'organization',
                     'time_range_start',
                     'time_range_end']

    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    # Write dataframe to csv
    df.to_csv('linkedin_follower_stats_daily.csv', encoding='utf-8')

    return "Data has been saved"
