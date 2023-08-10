import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
from datetime import date
from google.cloud import bigquery
from helpers import (return_active_token,
                     get_from_api,
                     create_bq_table,
                     region_uri_to_en)

'''
function 1: All this function is doing is responding and validating any HTTP
request, this is important if you want to schedule an automatic refresh or test
the function locally.
'''


def validate_http(request):

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and request_args:
        pull_to_prod()
        return f'Data pull complete'
    elif request_json and request_args:
        pull_to_prod()
        return f'Data pull complete'
    else:
        pull_to_prod()
        return f'Data pull complete'


'''
function 2: This is the code to pull data from the API and store it in a DataFrame
'''


def get_api_data():

    # URL for page stats
    page_stats_url = 'https://api.linkedin.com/rest/organizationPageStatistics?q=organization&organization=urn:li:organization:30216658'

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    page_stats_data = get_from_api(page_stats_url, headers)
    endpoint = page_stats_data['elements'][0]

    # Fetch staff_count_range data only
    staff_count_range_data = endpoint['pageStatisticsByStaffCountRange']

    # Convert into a dataframe
    df = pd.json_normalize(staff_count_range_data)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['staffCountRange',
            'pull_date',
            'pageStatistics.views.mobileProductsPageViews.pageViews',
            'pageStatistics.views.allDesktopPageViews.pageViews',
            'pageStatistics.views.insightsPageViews.pageViews',
            'pageStatistics.views.mobileAboutPageViews.pageViews',
            'pageStatistics.views.allMobilePageViews.pageViews',
            'pageStatistics.views.jobsPageViews.pageViews',
            'pageStatistics.views.productsPageViews.pageViews',
            'pageStatistics.views.desktopProductsPageViews.pageViews',
            'pageStatistics.views.peoplePageViews.pageViews',
            'pageStatistics.views.overviewPageViews.pageViews',
            'pageStatistics.views.mobileOverviewPageViews.pageViews',
            'pageStatistics.views.lifeAtPageViews.pageViews',
            'pageStatistics.views.desktopOverviewPageViews.pageViews',
            'pageStatistics.views.mobileCareersPageViews.pageViews',
            'pageStatistics.views.allPageViews.pageViews',
            'pageStatistics.views.mobileJobsPageViews.pageViews',
            'pageStatistics.views.careersPageViews.pageViews',
            'pageStatistics.views.mobileLifeAtPageViews.pageViews',
            'pageStatistics.views.desktopJobsPageViews.pageViews',
            'pageStatistics.views.desktopPeoplePageViews.pageViews',
            'pageStatistics.views.aboutPageViews.pageViews',
            'pageStatistics.views.desktopAboutPageViews.pageViews',
            'pageStatistics.views.mobilePeoplePageViews.pageViews',
            'pageStatistics.views.desktopInsightsPageViews.pageViews',
            'pageStatistics.views.desktopCareersPageViews.pageViews',
            'pageStatistics.views.desktopLifeAtPageViews.pageViews',
            'pageStatistics.views.mobileInsightsPageViews.pageViews']

    df = df[cols]

    # Rename columns
    old_col_names = ['staffCountRange',
                     'pageStatistics.views.mobileProductsPageViews.pageViews',
                     'pageStatistics.views.allDesktopPageViews.pageViews',
                     'pageStatistics.views.insightsPageViews.pageViews',
                     'pageStatistics.views.mobileAboutPageViews.pageViews',
                     'pageStatistics.views.allMobilePageViews.pageViews',
                     'pageStatistics.views.jobsPageViews.pageViews',
                     'pageStatistics.views.productsPageViews.pageViews',
                     'pageStatistics.views.desktopProductsPageViews.pageViews',
                     'pageStatistics.views.peoplePageViews.pageViews',
                     'pageStatistics.views.overviewPageViews.pageViews',
                     'pageStatistics.views.mobileOverviewPageViews.pageViews',
                     'pageStatistics.views.lifeAtPageViews.pageViews',
                     'pageStatistics.views.desktopOverviewPageViews.pageViews',
                     'pageStatistics.views.mobileCareersPageViews.pageViews',
                     'pageStatistics.views.allPageViews.pageViews',
                     'pageStatistics.views.mobileJobsPageViews.pageViews',
                     'pageStatistics.views.careersPageViews.pageViews',
                     'pageStatistics.views.mobileLifeAtPageViews.pageViews',
                     'pageStatistics.views.desktopJobsPageViews.pageViews',
                     'pageStatistics.views.desktopPeoplePageViews.pageViews',
                     'pageStatistics.views.aboutPageViews.pageViews',
                     'pageStatistics.views.desktopAboutPageViews.pageViews',
                     'pageStatistics.views.mobilePeoplePageViews.pageViews',
                     'pageStatistics.views.desktopInsightsPageViews.pageViews',
                     'pageStatistics.views.desktopCareersPageViews.pageViews',
                     'pageStatistics.views.desktopLifeAtPageViews.pageViews',
                     'pageStatistics.views.mobileInsightsPageViews.pageViews']

    new_col_names = ['staff_count_range',
                     'mobile_products_page_views',
                     'all_desktop_page_views',
                     'insights_page_views',
                     'mobile_about_page_views',
                     'all_mobile_page_views',
                     'jobs_page_views',
                     'products_page_views',
                     'desktop_products_page_views',
                     'people_page_views',
                     'overview_page_views',
                     'mobile_overview_page_views',
                     'life_at_page_views',
                     'desktop_overview_page_views',
                     'mobile_careers_page_views',
                     'all_page_views',
                     'mobile_jobs_page_views',
                     'careers_page_views',
                     'mobile_life_at_page_views',
                     'desktop_jobs_page_views',
                     'desktop_people_page_views',
                     'about_page_views',
                     'desktop_about_page_views',
                     'mobile_people_page_views',
                     'desktop_insights_page_views',
                     'desktop_careers_page_views',
                     'desktop_life_at_page_views',
                     'mobile_insights_page_views']

    # Rename cols
    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_page_stats_by_region', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("staff_count_range", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pull_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("mobile_products_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_desktop_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("insights_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_about_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_mobile_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("jobs_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("products_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_products_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("people_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("overview_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_overview_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("life_at_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_overview_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_jobs_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_life_at_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_jobs_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_people_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("about_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_about_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_people_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_insights_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_careers_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_life_at_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_insights_page_views", "INTEGER", mode="NULLABLE")
    ]

    table = create_bq_table(schema)
    df = get_api_data()
    bq_load(table, df, 'marketing_staging')

    return "Data has been loaded to BigQuery"


'''
function 5: This pulls the data to a csv - used for testing
'''


def pull_to_csv():

    df = get_api_data()
    df.to_csv("test.csv")

    return "Data has been saved"


'''
function 6: This function just converts your pandas dataframe into a bigquery
table, you'll also need to designate the name and location of the table in the
variable names below.
'''


def bq_load(key, value, dataset_name):

    project_name = 'marketing-bd-379302'
    table_name = key

    value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name),
                 project_id=project_name, if_exists='append')
