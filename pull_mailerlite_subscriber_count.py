import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):

    # Fetch total number of subscribers
    url = 'https://connect.mailerlite.com/api/subscribers?limit=0'
    headers = {
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI0IiwianRpIjoiOTMzMWMwZmMzZDRjMjQyOTU2NTU0NmE1M2IzNjQ3ZWI2NDgzOGUyNjcyZWU1MTRkMjFhZGQ2MjVlZTMwMjAyN2M5Nzg5NDU4YjZjYjdlMjgiLCJpYXQiOjE2NzkyODEwNjkuMzA5NjAxLCJuYmYiOjE2NzkyODEwNjkuMzA5NjAzLCJleHAiOjQ4MzQ5NTQ2NjkuMzAzODkyLCJzdWIiOiIzMTA3MDIiLCJzY29wZXMiOltdfQ.eV66qRiAJ7Xk0-pEvcNL9DuLF9EkV7jonvmdGhZ_53o8_ErxHIN4woXqW5xSj784kPS5g7gJzfSUfpP-7pwkp1KUDEfxHpwROQf2tKjQA3SxOUSz64qC7X7jkfe490RL9WJiLUE3NaIfqoLBGOrdI4KH-k6onkAxVzS-DdFXYkJMCner30YjStD7mt8UyqQQqXeuvOrtRkonIGzvj_-faQVG-DtNZrfA4-EilRssvKuvBisxwQWwKn9PWds1Jqyetn7vnb9Fgll7gZomGO0sYMcZmfFD7lD6eGnXAPKA6AJ1IGS5o-OI0UcvsdQmREpbstO9FfeC2qxfK_8GJlaOjE2XEagnGexC-p_yz7jZNtT2ki-ueFaVeglhzLPfgzYCubc8fyrrP9SeCmV99FilarLpmHjOW7mWmihhLqI4y4-L1evVpJP8_gkOZhe6earyH0UXuu0ZiGczvtTQYnyEbKLNd80qA5bVBNY1LAMEc8qw67Dmm6AhphA96VFInzIlgnIvOvfCd6Rd88Gl04llkg_7hYH3hfXNqeYxWkU-mYhcWIDh1LRqpHue6L8CnWQMIz9Tiz46QSVVOrCPFUQvcSUAYwfLQmuW_uG32v6D5FqOz5K2StCWTrlxb5w55iNF6ioltkjHVtjOKyP2PdlCnCyj_6CPBgJSZHQYa-sv1rM"
    }

    subscribers_data = get_from_api(url, headers)

    df = pd.DataFrame(subscribers_data, index=[0])
    df["pull_date"] = date.today()

    # Reorder columns
    cols = ['pull_date',
            'total']
    df = df[cols]

    # Rename columns
    old_col_names = ['total']
    new_col_names = ['follower_count']
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Write dataframe to csv
    df.to_csv('mailerlite_subscriber_count.csv', encoding='utf-8')

    return "Data has been saved"
