import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Fetch daily data from organization share API
    lifetime_share_url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:30216658"

    lifetime_share_headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        "Linkedin-Version": "202302"
    }

    lifetime_share_request = requests.get(lifetime_share_url, headers=lifetime_share_headers)
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
