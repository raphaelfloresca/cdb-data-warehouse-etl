from datetime import date
import pandas as pd
from helpers.helpers import get_from_api


# Main entry point for the cloud function
def pull_from_api(self):
    campaigns_api_url = 'https://connect.mailerlite.com/api/campaigns?filter[status]=sent'

    headers = {
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0IiwianRpIjoiOTMzMWMwZmMzZDRjMjQyOTU2NTU0NmE1M2IzNjQ3ZWI2NDgzOGUyNjcyZWU1MTRkMjFhZGQ2MjVlZTMwMjAyN2M5Nzg5NDU4YjZjYjdlMjgiLCJpYXQiOjE2NzkyODEwNjkuMzA5NjAxLCJuYmYiOjE2NzkyODEwNjkuMzA5NjAzLCJleHAiOjQ4MzQ5NTQ2NjkuMzAzODkyLCJzdWIiOiIzMTA3MDIiLCJzY29wZXMiOltdfQ.eV66qRiAJ7Xk0-pEvcNL9DuLF9EkV7jonvmdGhZ_53o8_ErxHIN4woXqW5xSj784kPS5g7gJzfSUfpP-7pwkp1KUDEfxHpwROQf2tKjQA3SxOUSz64qC7X7jkfe490RL9WJiLUE3NaIfqoLBGOrdI4KH-k6onkAxVzS-DdFXYkJMCner30YjStD7mt8UyqQQqXeuvOrtRkonIGzvj_-faQVG-DtNZrfA4-EilRssvKuvBisxwQWwKn9PWds1Jqyetn7vnb9Fgll7gZomGO0sYMcZmfFD7lD6eGnXAPKA6AJ1IGS5o-OI0UcvsdQmREpbstO9FfeC2qxfK_8GJlaOjE2XEagnGexC-p_yz7jZNtT2ki-ueFaVeglhzLPfgzYCubc8fyrrP9SeCmV99FilarLpmHjOW7mWmihhLqI4y4-L1evVpJP8_gkOZhe6earyH0UXuu0ZiGczvtTQYnyEbKLNd80qA5bVBNY1LAMEc8qw67Dmm6AhphA96VFInzIlgnIvOvfCd6Rd88Gl04llkg_7hYH3hfXNqeYxWkU-mYhcWIDh1LRqpHue6L8CnWQMIz9Tiz46QSVVOrCPFUQvcSUAYwfLQmuW_uG32v6D5FqOz5K2StCWTrlxb5w55iNF6ioltkjHVtjOKyP2PdlCnCyj_6CPBgJSZHQYa-sv1rM",
    }

    # Get all data from API
    campaigns_data = get_from_api(campaigns_api_url, headers, 'data')

    # Get data from all campaigns
    campaigns_names = []
    campaigns_ids = []
    campaigns_stats = []

    for data in campaigns_data:
        campaigns_names.append(data['name'])
        campaigns_ids.append(data['id'])
        campaigns_stats.append(data['stats'])

    campaigns_names_series = pd.Series(campaigns_names)
    campaigns_ids_series = pd.Series(campaigns_ids)
    df = pd.json_normalize(campaigns_stats)
    df['name'] = campaigns_names_series
    df['id'] = campaigns_ids_series
    # All data pulled associated with the date of the API request - allows for partitioning later
    df["pull_date"] = pd.to_datetime(date.today())

    cols = ['id',
            'name',
            'pull_date',
            'sent',
            'opens_count',
            'unique_opens_count',
            'clicks_count',
            'unique_clicks_count',
            'unsubscribes_count',
            'spam_count',
            'hard_bounces_count',
            'soft_bounces_count',
            'social_interactions_count',
            'click_to_open_rate.float',
            'open_rate.float',
            'click_rate.float',
            'unsubscribe_rate.float',
            'spam_rate.float',
            'hard_bounce_rate.float',
            'soft_bounce_rate.float',
            'forward_rate.float',
            'social_interaction_rate.float']

    # Reordered columns
    df = df[cols]

    # Rename columns
    df = df.rename(columns={'click_to_open_rate.float': 'click_to_open_rate',
                            'open_rate.float': 'open_rate',
                            'click_rate.float': 'click_rate',
                            'unsubscribe_rate.float': 'unsubscribe_rate',
                            'spam_rate.float': 'spam_rate',
                            'hard_bounce_rate.float': 'hard_bounce_rate',
                            'soft_bounce_rate.float': 'soft_bounce_rate',
                            'forward_rate.float': 'forward_rate',
                            'social_interaction_rate.float': 'social_interaction_rate'
                            })

    # Write dataframe to csv
    df.to_csv('mailerlite_campaigns.csv', encoding='utf-8')

    return "Data has been saved"
