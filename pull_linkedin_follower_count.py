import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URL for follower count
    follower_count_url = 'https://api.linkedin.com/rest/networkSizes/urn:li:organization:30216658?edgeType=CompanyFollowedByMember'

    headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    follower_count_data = get_from_api(follower_count_url, headers)

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

    # Write dataframe to csv
    df.to_csv('linkedin_follower_count.csv', encoding='utf-8')

    return "Data has been saved"