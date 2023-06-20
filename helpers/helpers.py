import requests
import urllib.parse
from google.cloud import secretmanager


# Take metrics from IG posts and format into dictionary
def format_data_dict(raw_data):
  data_dict = {}

  for metric in raw_data:
    data_dict[metric['name']] = metric['values'][0]['value']

  return data_dict


# Take action breakdown metrics from IG posts and format into dictionary
def format_action_breakdown_dict(raw_data):
  breakdown_dict = {'bio_link_clicked': 0, 'call': 0, 'direction': 0, 'email': 0, 'other': 0, 'text': 0}

  for breakdown in raw_data:
    breakdown_dict[breakdown['dimension_values'][0]] = breakdown['value']

  return breakdown_dict


# Take navigation breakdown metrics frrom IG posts and format into dictionary
def format_navigation_breakdown_dict(raw_data):
  breakdown_dict = {'automatic_forward': 0, 'tap_back': 0, 'tap_exit': 0, 'tap_forward': 0, 'swipe_back': 0, 'swipe_down': 0, 'swipe_forward': 0, 'swipe_up': 0}

  for breakdown in raw_data:
    breakdown_dict[breakdown['dimension_values'][0]] = breakdown['value']

  return breakdown_dict


# Return v2 secret from Secret Manager
def return_secret(project_id, secret_id, version_id="2"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode('UTF-8')


# Create a new secret version
def add_new_secret_version(project_id, secret_id, payload):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Convert the string payload into a bytes. This step can be omitted if you
    # pass in bytes instead of a str for the payload argument.
    payload = payload.encode("UTF-8")

    # Calculate payload checksum. Passing a checksum in add-version request
    # is optional.
    crc32c = google_crc32c.Checksum()
    crc32c.update(payload)

    # Add the secret version.
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload, "data_crc32c": int(crc32c.hexdigest(), 16)},
        }
    )


# Return active token, otherwise make a new one if expired (only for LinkedIn)
def return_active_token(platform):
    if platform == 'li':
        # Check if the token is current
        check_token_url = "https://www.linkedin.com/oauth/v2/introspectToken"
        check_token_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        check_token_params = {
            'client_id': '864qixr6v91kph',
            'client_secret': return_secret('marketing-bd-379302', 'linkedin_api_client_secret'),
            'token': return_secret('marketing-bd-379302', 'linkedin_api_access_token')
        }
        check_token_request = requests.post(check_token_url, headers=check_token_headers, data=check_token_params)
        check_token_response = check_token_request.json()

        # If token is not active, refresh token
        if check_token_response['active'] != True:
            refresh_url = 'https://www.linkedin.com/oauth/v2/accessToken'
            refresh_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            refresh_params = {
                'grant_type': 'refresh_token',
                'refresh_token': return_secret('marketing-bd-379302, linkedin_api_refresh_token', 'latest'),
                'client_id': '864qixr6v91kph',
                'client_secret': return_secret('marketing-bd-379302', 'linkedin_api_client_secret', 'latest')
            }
            refresh_request = requests.post(refresh_url, headers=refresh_headers, data=refresh_params)
            refresh_response = refresh_request.json()
            new_token = refresh_response['access_token']

            # Create a new version of the API access token secret
            add_new_secret_version('marketing-bd-379302', 'linkedin_api_access_token', new_token)

            # Return new version of secret
            return return_secret('marketing-bd-379302', 'linkedin_api_access_token', 'latest')

        # Return current version of secret
        return check_token_params["token"]
    elif platform == 'ig':
        return return_secret('marketing-bd-379302', 'ig_access_token')


# Get data from an API
def get_from_api(url, headers=None, endpoint=None):
    request = requests.get(url, headers=headers)
    json = request.json()
    if not endpoint == None:
        json = json[endpoint]
        return json
    return json


# Get data from geo API (LinkedIn)
def get_from_geo_api(url, headers, ids):
    id_list = list(ids)
    clean_ids = []

    for geo_id in id_list:
        clean_ids.append(geo_id.lstrip('urn:li:geo:'))

    id_string = ','.join(clean_ids)
    complete_url = url + id_string + ")"

    request = requests.get(complete_url, headers=headers)
    json = request.json()

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
                    if 'count' in value:
                      social_metadata_row['comments_count'] = value['count']
                      social_metadata_row['comments_top_level_count'] = value['topLevelCount']
                    else:
                      continue
                if key == "commentsState":
                    social_metadata_row['comments_state'] = value

            print(value)

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


# Function to get readable region from region URI
def region_uri_to_en(data, uri):
    glossary = data['elements']

    for item in glossary:
        if item['$URN'] == uri:
            return item['name']['value']


# Function to get readable country from country URI
def country_uri_to_en(data, uri):
    glossary = data['elements']

    for item in glossary:
        if item['$URN'] == uri:
            return item['name']['value']


# Function to get readable geo from geo URI
def geo_uri_to_en(data, uri):
    # print(uri)
    glossary = data['results']
    # print(glossary)

    for geo in glossary.values():
        if str(geo['id']) == uri:
            return geo['defaultLocalizedName']['value']
