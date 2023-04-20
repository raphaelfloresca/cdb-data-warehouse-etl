import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Start with first element
    all_shares_start_url = 'https://api.linkedin.com/v2/shares?q=owners&owners=urn:li:organization:30216658&sortBy=LAST_MODIFIED&sharesPerOwner=1000&start=0&count=50'

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
    all_shares_start_data = get_from_api(all_shares_start_url, headers)
    all_shares_start_data_filtered = filter_no_id(all_shares_start_data['elements'], 'activity')
    all_shares_flattened_data = list(map(transform_shares, all_shares_start_data_filtered))
    all_shares_next_data = get_paged_data(all_shares_start_data, headers)

    # Create base DataFrame with all posts
    df = pd.DataFrame(all_shares_flattened_data)
    df_next = pd.DataFrame(all_shares_next_data)
    df = pd.concat([df, df_next])
    df = df.drop_duplicates(subset=['activity']).reset_index(drop=True)

    # Convert last modified time to datetime format
    df['lastModified_time'] = pd.to_datetime(df['lastModified_time'], unit='ms')

    # Convert created time to datetime format
    df['created_time'] = pd.to_datetime(df['created_time'], unit='ms')

    # Get share IDs from activity IDs
    share_list = get_share_list(df, headers)
    activity_to_share_df = pd.concat([pd.Series(share_list, name='share'), df['activity']], axis=1)

    # Append share IDs to DataFrame
    df_with_share_id = df.join(
        activity_to_share_df.set_index(["activity"]),
        on=["activity"],
        how="inner",
        lsuffix="_x",
        rsuffix="_y"
    )

    # Split share list into chunks of 50
    split_share_list = list(split(share_list, 50))

    # Get all social metadata
    chunked_social_metadata = get_chunked_social_metadata(split_share_list, x_restli_2_0_headers)
    social_metadata_list = get_all_social_metadata(chunked_social_metadata)
    social_metadata_df = pd.DataFrame(social_metadata_list)

    # Append all social metadata to DataFrame
    df_with_social_metadata = df_with_share_id.join(
        social_metadata_df.set_index(["share"]),
        on=["share"],
        how="inner",
        lsuffix="_x",
        rsuffix="_y"
    )

    # All data pulled associated with the date of the API request - allows for partitioning laterr
    df_with_social_metadata["pull_date"] = pd.to_datetime(date.today())

    cols = ['activity',
            'share',
            'pull_date',
            'created_time',
            'lastModified_time',
            'created_actor',
            'text',
            'like_count',
            'praise_count',
            'maybe_count',
            'empathy_count',
            'interest_count',
            'appreciation_count',
            'comments_state',
            'comments_count',
            'comments_top_level_count']

    df_with_social_metadata = df_with_social_metadata[cols]

    df_with_social_metadata = df_with_social_metadata.rename(columns={'lastModified_time': 'last_modified_time'})

    # Write dataframe to csv
    df_with_social_metadata.to_csv('linkedin_social_metadata_shares.csv', encoding='utf-8')

    return "Data has been saved"

