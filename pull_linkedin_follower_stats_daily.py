import pandas as pd
from flatten_json import flatten
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Fetch daily data from page statistics API
    url = "https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:2414183&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start=1648425600000&timeIntervals.timeRange.end=1679875200000"

    headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        "Linkedin-Version": "202302"
    }

    # Get all data from API
    data = get_from_api(url, headers)
    endpoint = data['elements']

    # Flatten json
    daily_data_flatten = map(flatten, endpoint)

    # Create dataframe
    df = pd.DataFrame(list(daily_data_flatten))

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

    # Assign new column names
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    # Write dataframe to csv
    df.to_csv('linkedin_follower_stats_daily.csv', encoding='utf-8')

    return "Data has been saved"
