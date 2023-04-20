import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Fetch daily data from organization share API
    lifetime_share_url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658"

    headers = {
        "Authorization": "Bearer {}".format(return_active_token()),
        'Linkedin-Version': '202302'
    }

    lifetime_share_request = requests.get(lifetime_share_url, headers=headers)
    df = pd.json_normalize(lifetime_share_request.json()['elements'])

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['pull_date',
            'totalShareStatistics.uniqueImpressionsCount',
            'totalShareStatistics.shareCount',
            'totalShareStatistics.shareMentionsCount',
            'totalShareStatistics.engagement',
            'totalShareStatistics.clickCount',
            'totalShareStatistics.likeCount',
            'totalShareStatistics.impressionCount',
            'totalShareStatistics.commentMentionsCount',
            'totalShareStatistics.commentCount']

    df = df[cols]

    # Rename columns
    old_col_names = ['totalShareStatistics.uniqueImpressionsCount',
                     'totalShareStatistics.shareCount',
                     'totalShareStatistics.shareMentionsCount',
                     'totalShareStatistics.engagement',
                     'totalShareStatistics.clickCount',
                     'totalShareStatistics.likeCount',
                     'totalShareStatistics.impressionCount',
                     'totalShareStatistics.commentMentionsCount',
                     'totalShareStatistics.commentCount']

    new_col_names = ['unique_impressions_count',
                     'share_count',
                     'share_mentions_count',
                     'engagement',
                     'click_count',
                     'like_count',
                     'impression_count',
                     'comment_mentions_count',
                     'comment_count']

    # Assign new column names
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Write dataframe to csv
    df.to_csv('linkedin_shares_lifetime.csv', encoding='utf-8')

    return "Data has been saved"
