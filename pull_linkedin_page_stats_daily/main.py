import pandas as pd
import sys
import json
import requests
import pandas_gbq
import gcsfs
import os
from datetime import date
from google.cloud import bigquery
from helpers import return_active_token, get_from_api, create_bq_table
from flatten_json import flatten
from datetime import date, datetime, timedelta
import calendar
from dotenv import load_dotenv


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

    # Get start and end dates
    end_date = date.today() - timedelta(7)
    start_date = date.today()
    midnight_time = datetime.min.time()

    end_datetime = datetime.combine(end_date, midnight_time)
    start_datetime = datetime.combine(start_date, midnight_time)

    time_range_start = calendar.timegm(start_datetime.utctimetuple()) * 1000
    time_range_end = calendar.timegm(end_datetime.utctimetuple()) * 1000

    # Fetch daily data from page statistics API
    url = "https://api.linkedin.com/rest/organizationPageStatistics?q=organization&organization=urn:li:organization:30216658&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start={}&timeIntervals.timeRange.end={}".format(time_range_start, time_range_end)

    # Headers
    headers = {
        "Authorization": "Bearer {}".format(os.environ.get("TOKEN")),
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    data = get_from_api(url, headers)
    endpoint = data['elements']

    # Flatten json
    daily_data_flatten = map(flatten, endpoint)

    # Create dataframe
    df = pd.DataFrame(list(daily_data_flatten))

    # Assign new column names
    old_col_names = ['totalPageStatistics_clicks_mobileCustomButtonClickCounts_0_customButtonType',
                     'totalPageStatistics_clicks_mobileCustomButtonClickCounts_0_clicks',
                     'totalPageStatistics_clicks_careersPageClicks_careersPagePromoLinksClicks',
                     'totalPageStatistics_clicks_careersPageClicks_careersPageBannerPromoClicks',
                     'totalPageStatistics_clicks_careersPageClicks_careersPageJobsClicks',
                     'totalPageStatistics_clicks_careersPageClicks_careersPageEmployeesClicks',
                     'totalPageStatistics_clicks_desktopCustomButtonClickCounts_0_customButtonType',
                     'totalPageStatistics_clicks_desktopCustomButtonClickCounts_0_clicks',
                     'totalPageStatistics_clicks_mobileCareersPageClicks_careersPageJobsClicks',
                     'totalPageStatistics_clicks_mobileCareersPageClicks_careersPagePromoLinksClicks',
                     'totalPageStatistics_clicks_mobileCareersPageClicks_careersPageEmployeesClicks',
                     'totalPageStatistics_views_mobileProductsPageViews_pageViews',
                     'totalPageStatistics_views_mobileProductsPageViews_uniquePageViews',
                     'totalPageStatistics_views_allDesktopPageViews_pageViews',
                     'totalPageStatistics_views_allDesktopPageViews_uniquePageViews',
                     'totalPageStatistics_views_insightsPageViews_pageViews',
                     'totalPageStatistics_views_insightsPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileAboutPageViews_pageViews',
                     'totalPageStatistics_views_mobileAboutPageViews_uniquePageViews',
                     'totalPageStatistics_views_allMobilePageViews_pageViews',
                     'totalPageStatistics_views_allMobilePageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopProductsPageViews_pageViews',
                     'totalPageStatistics_views_desktopProductsPageViews_uniquePageViews',
                     'totalPageStatistics_views_productsPageViews_pageViews',
                     'totalPageStatistics_views_productsPageViews_uniquePageViews',
                     'totalPageStatistics_views_jobsPageViews_pageViews',
                     'totalPageStatistics_views_jobsPageViews_uniquePageViews',
                     'totalPageStatistics_views_peoplePageViews_pageViews',
                     'totalPageStatistics_views_peoplePageViews_uniquePageViews',
                     'totalPageStatistics_views_overviewPageViews_pageViews',
                     'totalPageStatistics_views_overviewPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileOverviewPageViews_pageViews',
                     'totalPageStatistics_views_mobileOverviewPageViews_uniquePageViews',
                     'totalPageStatistics_views_lifeAtPageViews_pageViews',
                     'totalPageStatistics_views_lifeAtPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopOverviewPageViews_pageViews',
                     'totalPageStatistics_views_desktopOverviewPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileCareersPageViews_pageViews',
                     'totalPageStatistics_views_mobileCareersPageViews_uniquePageViews',
                     'totalPageStatistics_views_allPageViews_pageViews',
                     'totalPageStatistics_views_allPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileJobsPageViews_pageViews',
                     'totalPageStatistics_views_mobileJobsPageViews_uniquePageViews',
                     'totalPageStatistics_views_careersPageViews_pageViews',
                     'totalPageStatistics_views_careersPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileLifeAtPageViews_pageViews',
                     'totalPageStatistics_views_mobileLifeAtPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopJobsPageViews_pageViews',
                     'totalPageStatistics_views_desktopJobsPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopPeoplePageViews_pageViews',
                     'totalPageStatistics_views_desktopPeoplePageViews_uniquePageViews',
                     'totalPageStatistics_views_aboutPageViews_pageViews',
                     'totalPageStatistics_views_aboutPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopAboutPageViews_pageViews',
                     'totalPageStatistics_views_desktopAboutPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobilePeoplePageViews_pageViews',
                     'totalPageStatistics_views_mobilePeoplePageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopInsightsPageViews_pageViews',
                     'totalPageStatistics_views_desktopInsightsPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopCareersPageViews_pageViews',
                     'totalPageStatistics_views_desktopCareersPageViews_uniquePageViews',
                     'totalPageStatistics_views_desktopLifeAtPageViews_pageViews',
                     'totalPageStatistics_views_desktopLifeAtPageViews_uniquePageViews',
                     'totalPageStatistics_views_mobileInsightsPageViews_pageViews',
                     'totalPageStatistics_views_mobileInsightsPageViews_uniquePageViews',
                     'timeRange_start',
                     'timeRange_end',
                     'organization']

    new_col_names = ['mobile_custom_button_click_counts_0_custom_button_type',
                     'mobile_custom_button_click_counts_0_clicks',
                     'careers_page_clicks_careers_page_promo_links_clicks',
                     'careers_page_clicks_careers_page_banner_promo_clicks',
                     'careers_page_clicks_careers_page_jobs_clicks',
                     'careers_page_clicks_careers_page_employees_clicks',
                     'desktop_custom_button_click_counts_0_custom_button_type',
                     'desktop_custom_button_click_counts_0_clicks',
                     'mobile_careers_page_clicks_careers_page_jobs_clicks',
                     'mobile_careers_page_clicks_careers_page_promo_links_clicks',
                     'mobile_careers_page_clicks_careers_page_employees_clicks',
                     'mobile_products_page_views_page_views',
                     'mobile_products_page_views_unique_page_views',
                     'all_desktop_page_views_page_views',
                     'all_desktop_page_views_unique_page_views',
                     'insights_page_views_page_views',
                     'insights_page_views_unique_page_views',
                     'mobile_about_page_views_page_views',
                     'mobile_about_page_views_unique_page_views',
                     'all_mobile_page_views_page_views',
                     'all_mobile_page_views_unique_page_views',
                     'desktop_products_page_views_page_views',
                     'desktop_products_page_views_unique_page_views',
                     'products_page_views_page_views',
                     'products_page_views_unique_page_views',
                     'jobs_page_views_page_views',
                     'jobs_page_views_unique_page_views',
                     'people_page_views_page_views',
                     'people_page_views_unique_page_views',
                     'overview_page_views_page_views',
                     'overview_page_views_unique_page_views',
                     'mobile_overview_page_views_page_views',
                     'mobile_overview_page_views_unique_page_views',
                     'life_at_page_views_page_views',
                     'life_at_page_views_unique_page_views',
                     'desktop_overview_page_views_page_views',
                     'desktop_overview_page_views_unique_page_views',
                     'mobile_careers_page_views_page_views',
                     'mobile_careers_page_views_unique_page_views',
                     'all_page_views_page_views',
                     'all_page_views_unique_page_views',
                     'mobile_jobs_page_views_page_views',
                     'mobile_jobs_page_views_unique_page_views',
                     'careers_page_views_page_views',
                     'careers_page_views_unique_page_views',
                     'mobile_life_at_page_views_page_views',
                     'mobile_life_at_page_views_unique_page_views',
                     'desktop_jobs_page_views_page_views',
                     'desktop_jobs_page_views_unique_page_views',
                     'desktop_people_page_views_page_views',
                     'desktop_people_page_views_unique_page_views',
                     'about_page_views_page_views',
                     'about_page_views_unique_page_views',
                     'desktop_about_page_views_page_views',
                     'desktop_about_page_views_unique_page_views',
                     'mobile_people_page_views_page_views',
                     'mobile_people_page_views_unique_page_views',
                     'desktop_insights_page_views_page_views',
                     'desktop_insights_page_views_unique_page_views',
                     'desktop_careers_page_views_page_views',
                     'desktop_careers_page_views_unique_page_views',
                     'desktop_life_at_page_views_page_views',
                     'desktop_life_at_page_views_unique_page_views',
                     'mobile_insights_page_views_page_views',
                     'mobile_insights_page_views_unique_page_views',
                     'time_range_start',
                     'time_range_end',
                     'organization']

    new_cols = dict(zip(old_col_names, new_col_names))
    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    return df


'''
function 3: This pulls the data to the production database
'''


def pull_to_prod():

    df = get_api_data()
    bq_load('linkedin_page_stats_daily', df, 'cdb_marketing_data')

    return "Data has been loaded to BigQuery"


'''
function 4: This pulls the data to the staging database - used for testing
'''


def pull_to_staging():

    schema = [
        bigquery.SchemaField("mobile_custom_button_click_counts_0_custom_button_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mobile_custom_button_click_counts_0_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_clicks_careers_page_promo_links_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_clicks_careers_page_banner_promo_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_clicks_careers_page_jobs_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_clicks_careers_page_employees_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_custom_button_click_counts_0_custom_button_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("desktop_custom_button_click_counts_0_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_clicks_careers_page_jobs_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_clicks_careers_page_promo_links_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_clicks_careers_page_employees_clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_products_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_products_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_desktop_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_desktop_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("insights_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("insights_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_about_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_about_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_mobile_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_mobile_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_products_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_products_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("products_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("products_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("jobs_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("jobs_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("people_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("people_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("overview_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("overview_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_overview_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_overview_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("life_at_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("life_at_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_overview_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_overview_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_careers_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("all_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_jobs_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_jobs_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("careers_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_life_at_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_life_at_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_jobs_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_jobs_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_people_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_people_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("about_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("about_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_about_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_about_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_people_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_people_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_insights_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_insights_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_careers_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_careers_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_life_at_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("desktop_life_at_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_insights_page_views_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("mobile_insights_page_views_unique_page_views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("time_range_start", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("time_range_end", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("organization", "STRING", mode="NULLABLE")
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
