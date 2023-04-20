import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URL for follower count
    follower_count_url = 'https://api.linkedin.com/rest/networkSizes/urn:li:organization:30216658?edgeType=CompanyFollowedByMember'

    headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
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
