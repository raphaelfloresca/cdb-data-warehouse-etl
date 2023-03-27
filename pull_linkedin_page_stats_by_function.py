import pandas as pd
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # URL for page stats and
    page_stats_url = 'https://api.linkedin.com/rest/organizationPageStatistics?q=organization&organization=urn:li:organization:30216658'
    function_url = 'https://api.linkedin.com/v2/functions/'

    headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        'Linkedin-Version': '202302'
    }

    # Get all data from API
    page_stats_data = get_from_api(page_stats_url, headers)
    endpoint = page_stats_data['elements'][0]
    function_index = get_from_api(function_url, headers)

    # Fetch function data only
    function_data = endpoint['pageStatisticsByFunction']

    # Replace URI with readable name
    for function in function_data:
        function['function'] = sen_uri_to_en(function_index, function['function'])

    # Convert into a dataframe
    df = pd.json_normalize(function_data)

    # Get pull date
    df["pull_date"] = pd.to_datetime(date.today())

    # Reorder columns
    cols = ['function',
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
    old_col_names = ['pageStatistics.views.mobileProductsPageViews.pageViews',
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

    new_col_names = ['mobile_products_page_views',
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

    # Write dataframe to csv
    df.to_csv('linkedin_page_stats_function.csv', encoding='utf-8')

    return "Data has been saved"
