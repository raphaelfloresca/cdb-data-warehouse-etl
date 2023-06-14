import requests
import pandas as pd
from datetime import date, datetime, timedelta
import calendar
from helpers.helpers import *

# Main entry point for the cloud function
def pull_from_api(self):
    # Define start and end dates for pulling in daily data
    end_date = date.today()
    start_date = date.today() - timedelta(1)
    midnight_time = datetime.min.time()
    end_datetime = datetime.combine(end_date, midnight_time)
    start_datetime = datetime.combine(start_date, midnight_time)
    #time_range_start = '1680825600' calendar.timegm(start_datetime.utctimetuple())
    #time_range_end = '1683504000' calendar.timegm(end_datetime.utctimetuple())
    time_range_start = calendar.timegm(start_datetime.utctimetuple())
    time_range_end = calendar.timegm(end_datetime.utctimetuple())

    # Pull in data
    access_token = return_active_token('ig')
    url = "https://graph.facebook.com/v16.0/17841456374080288/insights?pretty=0&since={}&until={}&metric=impressions,reach,profile_views,email_contacts,get_directions_clicks,phone_call_clicks,text_message_clicks,website_clicks&period=day&access_token={}".format(time_range_start, time_range_end, access_token)
    data = get_from_api(url, endpoint='data')

    # Turn data into a normalized dataframe
    normalized_data = pd.json_normalize(data)

    # Initialize empty list of DataFrames which will hold data for a specific metric
    df_list = []

    # Create df list
    for index, row in normalized_data.iterrows():
        current_metric = row['name']
        df = pd.json_normalize(dict(row), 'values')
        df['name'] = current_metric
        df_list.append(df)

    # List of metrics to iterate over
    metric_list = ["impressions", "reach", "profile_views", "email_contacts", "get_directions_clicks", "phone_call_clicks", "text_message_clicks", "website_clicks"]

    # Index of current metric
    current_metric_index = 0

    # Drop names for each entry in DF list and rename data columns with the metric they represent
    for df in df_list:
        df_list[current_metric_index] = df.rename(columns={"value": metric_list[current_metric_index]})
        df_list[current_metric_index] = df_list[current_metric_index].drop("name", axis=1)
        current_metric_index += 1

    # Merge all metrics together
    account_metrics = pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(df_list[0], df_list[1], on='end_time'), df_list[2], on='end_time'), df_list[3], on='end_time'), df_list[4], on='end_time'), df_list[5], on='end_time'), df_list[6], on='end_time'), df_list[7], on='end_time')

    # Write dataframe to csv
    account_metrics.to_csv('ig_account_metrics.csv', encoding='utf-8')

    return "Data has been saved"
