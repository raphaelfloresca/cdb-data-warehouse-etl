import requests
import pandas as pd
from helpers.helpers import *

# Main entry point for the cloud function
def pull_from_api(self):
    url = "https://graph.facebook.com/v16.0/17841456374080288/insights?pretty=0&since=1680019200&until=1681228800&metric=impressions,reach,profile_views,email_contacts,get_directions_clicks,phone_call_clicks,text_message_clicks,website_clicks&period=day&access_token=EAANTIDDj7vQBAGBDrWnP96sf14dVvPmKkA4kpbiGg46JUpyQJWti7cnVMU3A4cGlE4WFsgQPiFZAUAhLz3EJ18ziRUP9GXOU7M1hlhaPIvBPjo61NyGGsB80g9IlEuEl2dnZBIB3NWIyaHMIZAtB9ZC31HXN5PhJPewRARrQHC2De8wFjwomPx582ioqZBlQCIPd2mPGo7zGcy0wnfS5qI74MKz7E6oQZD"

    request = requests.get(url)
    data = request.json()
    normalized_data = pd.json_normalize(data['data'])
    df_list = []

    for index, row in normalized_data.iterrows():
        current_metric = row['name']
        df = pd.json_normalize(dict(row), 'values')
        df['name'] = current_metric
        df_list.append(df)

    df_list[0] = df_list[0].rename(columns={"value": "impressions"})
    df_list[0] = df_list[0].drop("name", axis=1)

    df_list[1] = df_list[1].rename(columns={"value": "reach"})
    df_list[1] = df_list[1].drop("name", axis=1)

    df_list[2] = df_list[2].rename(columns={"value": "profile_views"})
    df_list[2] = df_list[2].drop("name", axis=1)

    df_list[3] = df_list[3].rename(columns={"value": "email_contacts"})
    df_list[3] = df_list[3].drop("name", axis=1)

    df_list[4] = df_list[4].rename(columns={"value": "get_directions_clicks"})
    df_list[4] = df_list[4].drop("name", axis=1)

    df_list[5] = df_list[5].rename(columns={"value": "phone_call_clicks"})
    df_list[5] = df_list[5].drop("name", axis=1)

    df_list[6] = df_list[6].rename(columns={"value": "text_message_clicks"})
    df_list[6] = df_list[6].drop("name", axis=1)

    df_list[7] = df_list[7].rename(columns={"value": "website_clicks"})
    df_list[7] = df_list[7].drop("name", axis=1)

    account_metrics = pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(df_list[0], df_list[1], on='end_time'), df_list[2], on='end_time'), df_list[3], on='end_time'), df_list[4], on='end_time'), df_list[5], on='end_time'), df_list[6], on='end_time'), df_list[7], on='end_time')


    # Write dataframe to csv
    account_metrics.to_csv('ig_account_metrics.csv', encoding='utf-8')

    return "Data has been saved"
