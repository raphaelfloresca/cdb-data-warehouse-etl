import pandas as pd
from flatten_json import flatten
from datetime import date
from helpers.helpers import *


# Main entry point for the cloud function
def pull_from_api(self):
    # Fetch daily data from page statistics API
    url = "https://api.linkedin.com/rest/organizationPageStatistics?q=organization&organization=urn:li:organization:30216658&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start=1648425600000&timeIntervals.timeRange.end=1679875200000"

    headers = {
        "Authorization": "Bearer AQU-ruNVnWde2coHnu5W0Yg6FBuFSJONyFurpmclnQQ4b4KR3kjFvt2SNAcjobZ-C-vMvAyL9rEHww8MYjWA0EVf7gCe5p7R2swy9_LCx_QZv2E1VBwoRbY9wKywFI2rKGFA0vt_klYbQjwtmzoDYLApejv6P0jt24GgY8MVCmyRlPrmApH55XSbrPDEwZk2u-GYettpDaQDsgz5IxmcVyLGEp9RdBMe5pDKm2kXAvx29Gk7IqJUCBWpEBBUiI3p8RAU46ttswyNkyA3ErCDWgxJC4W_-J4LEqORb3cjXdrJVTON-vn3poTnyfdc60WR__0KjjJJyBGdBjuZvL1IQiNrQvtuJA",
        "Linkedin-Version": "202302"
    }

    # Get all data from API
    data = get_from_api(url, headers)
    endpoint = data['elements']

    # Flatten json
    daily_data_flatten = map(flatten, endpoint)

    # Create dataframe
    df = pd.DataFrame(list(daily_data_flatten))

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

    # # Initialize dataframe
    # df = pd.DataFrame(data)
    #
    # # Get pull date
    # df["pull_date"] = pd.to_datetime(date.today())
    #
    # # Reorder columns
    # cols = ['pull_date',
    #         'firstDegreeSize']
    #
    # df = df[cols]
    #
    # # Rename columns
    # old_col_names = ['firstDegreeSize']
    # new_col_names = ['follower_count']
    #
    # # Assign new column names
    new_cols = dict(zip(old_col_names, new_col_names))

    df = df.rename(columns=new_cols)

    # Convert last modified time to datetime format
    df['time_range_start'] = pd.to_datetime(df['time_range_start'], unit='ms')
    df['time_range_end'] = pd.to_datetime(df['time_range_end'], unit='ms')

    # Write dataframe to csv
    df.to_csv('linkedin_page_stats_daily.csv', encoding='utf-8')

    return "Data has been saved"
