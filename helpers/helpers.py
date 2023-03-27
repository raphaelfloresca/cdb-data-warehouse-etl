import requests
import urllib.parse


# Get data from an API
def get_from_api(url, headers, endpoint=None):
    request = requests.get(url, headers=headers)
    json = request.json()
    if not endpoint == None:
        json = json[endpoint]
        return json
    return json


# Filter out data without a particular key
def filter_no_id(unfiltered_list, key_to_filter):
    return [item for item in unfiltered_list if key_to_filter in item]


# Take raw shares data and extract useful data points
def transform_shares(row):
    # Extract the first-level key value pairs
    row_subset = {key: value for key, value in row.items()
                  if key == "activity"
                  or key == "created"
                  or key == "text"
                  or key == "lastModified"}

    # Push required data into a dictionary
    clean_dict = {}

    # Schema: activity, created_actor, created_time, text, lastModified
    clean_dict['activity'] = row_subset['activity']
    clean_dict['created_actor'] = row_subset['created']['actor']
    clean_dict['created_time'] = row_subset['created']['time']
    clean_dict['text'] = row_subset['text']['text']
    clean_dict['lastModified_time'] = row_subset['lastModified']['time']

    return clean_dict


# Get data from the next page
def get_paged_data(start_data, headers):
    next_data_list = []
    next_page = next((item for item in start_data['paging']['links'] if item['rel'] == 'next'), None)

    while not next_page is None:
        # Next url
        next_url = 'https://api.linkedin.com' + str(next_page['href'])

        # Get next batch of data
        next_data = get_from_api(next_url, headers)

        # Filter out data without activity id
        next_data_filtered = filter_no_id(next_data['elements'], 'activity')
        next_flattened_data = list(map(transform_shares, next_data_filtered))

        # Append flattened data to data list
        next_data_list += next_flattened_data

        # Find the next page url
        next_page = next((item for item in next_data['paging']['links'] if item['rel'] == 'next'), None)

    return next_data_list


# Get a list of share IDs from a pandas Series of activity IDs
def get_share_list(df, headers):
    share_list = []

    for activity in list(df['activity']):
        get_share_urn_url = 'https://api.linkedin.com/v2/activities?ids=' + str(activity)
        get_share_urn_request = requests.get(get_share_urn_url, headers=headers)
        activity_to_share_urn = get_share_urn_request.json()['results'][str(activity)]['domainEntity']
        share_list.append(activity_to_share_urn)

    return share_list


# Split a list into sub-lists of a given size: implemented as LinkedIn can only reliably pull in batches of 50
def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]


# Encode a share list for batch GET request
def encode_share_list(share_list):
    encoded_list = [urllib.parse.quote(x) for x in share_list]
    share_str = ','.join(encoded_list)
    return share_str


# Get social metadata for a split share list
def get_chunked_social_metadata(split_share_list, headers):
    # Get social metadata
    all_social_metadata = []

    for share_list in split_share_list:
        share_str = encode_share_list(share_list)

        get_social_metadata_url = 'https://api.linkedin.com/rest/socialMetadata?ids=List(' + str(share_str) + ')'

        get_social_metadata_request = requests.get(get_social_metadata_url, headers=headers)
        get_social_metadata_batch = get_social_metadata_request.json()

        all_social_metadata.append(get_social_metadata_batch)

    return all_social_metadata


# Create a list of dictionaries containing social metadata for a given share ID
def get_all_social_metadata(chunked_social_metadata):
    social_metadata_list = []

    # Iterate over chunks
    for chunk in chunked_social_metadata:
        # Iterate over each activity's social metadata
        for share, metadata in chunk['results'].items():
            social_metadata_row = {}

            # Append activity to row
            social_metadata_row['share'] = share
            for key, value in metadata.items():
                if key == 'reactionSummaries':
                    if 'LIKE' in value:
                        social_metadata_row.update({'like_count': value['LIKE']['count']})
                    else:
                        social_metadata_row.update({'like_count': 0})

                    if 'PRAISE' in value:
                        social_metadata_row.update({'praise_count': value['PRAISE']['count']})
                    else:
                        social_metadata_row.update({'praise_count': 0})

                    if 'MAYBE' in value:
                        social_metadata_row.update({'maybe_count': value['MAYBE']['count']})
                    else:
                        social_metadata_row.update({'maybe_count': 0})

                    if 'EMPATHY' in value:
                        social_metadata_row.update({'empathy_count': value['EMPATHY']['count']})
                    else:
                        social_metadata_row.update({'empathy_count': 0})

                    if 'INTEREST' in value:
                        social_metadata_row.update({'interest_count': value['INTEREST']['count']})
                    else:
                        social_metadata_row.update({'interest_count': 0})

                    if 'APPRECIATION' in value:
                        social_metadata_row.update({'appreciation_count': value['APPRECIATION']['count']})
                    else:
                        social_metadata_row.update({'appreciation_count': 0})

                if key == 'commentSummary':
                    social_metadata_row['comments_count'] = value['count']
                    social_metadata_row['comments_top_level_count'] = value['topLevelCount']
                if key == "commentsState":
                    social_metadata_row['comments_state'] = value

            social_metadata_list.append(social_metadata_row)

    return social_metadata_list


# Function to get readable seniority from seniority URI
def sen_uri_to_en(data, uri):
    sen_glossary = data['elements']

    for item in sen_glossary:
        if item['$URN'] == uri:
            return item['name']['localized']['en_US']


# Function to get readable industry from industry URI
def industry_uri_to_en(data, uri):
    glossary = data['elements']

    for item in glossary:
        if item['$URN'] == uri:
            return item['name']['localized']['en_US']


# Function to get readable function from function URI
def function_uri_to_en(data, uri):
    glossary = data['elements']

    for item in glossary:
        if item['$URN'] == uri:
            return item['name']['localized']['en_US']
