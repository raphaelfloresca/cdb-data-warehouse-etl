import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URL for page stats and
    follower_stats_url = 'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658'
    seniority_url = 'https://api.linkedin.com/v2/seniorities/'

    headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    follower_stats_data = get_from_api(follower_stats_url, headers)
    endpoint = follower_stats_data['elements'][0]
    seniority_index = get_from_api(seniority_url, headers)

    # Fetch seniority data only
    seniority_data = endpoint['followerCountsBySeniority']

    # Replace URI with readable name
    for seniority in seniority_data:
        seniority['seniority'] = sen_uri_to_en(seniority_index, seniority['seniority'])

    # Convert into a dataframe
    df = pd.json_normalize(seniority_data)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['seniority',
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
    df.to_csv('linkedin_follower_stats_seniority.csv', encoding='utf-8')

    return "Data has been saved"