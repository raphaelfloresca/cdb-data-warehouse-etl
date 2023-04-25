import requests
import pandas as pd
from helpers.helpers import *

# Main entry point for the cloud function
def pull_from_api(self):
    access_token = return_active_token('ig')
    # Pull list of posts
    url = "https://graph.facebook.com/v16.0/17841456374080288/media?access_token={}".format(access_token)
    media_list = get_from_api(url, endpoint='data')
    media_list = [d['id'] for d in media_list]

    # Separate posts out by type
    post_list = []
    album_list = []
    reels_list = []
    story_list = []

    # Get info and metrics for each post and append them to appropriate list
    for media in media_list:
        url = "https://graph.facebook.com/v16.0/{}?fields=id,timestamp,media_product_type,media_type,permalink,like_count,comments_count,caption&access_token={}".format(media, access_token)
        media_info = get_from_api(url)

        if media_info['media_type'] == 'IMAGE' or media_info['media_type'] == 'VIDEO':
            url = "https://graph.facebook.com/v16.0/{}/insights?metric=follows,profile_visits,shares,total_interactions,impressions,reach,saved,video_views&access_token={}".format(media, access_token)
            media_metrics = get_from_api(url, endpoint='data')
            data_dict = format_data_dict(media_metrics)

            url = "https://graph.facebook.com/v16.0/{}/insights?metric=profile_activity&breakdown=action_type&access_token={}".format(media, access_token)
            post_profile_activity_breakdown = get_from_api(url)
            try:
                all_breakdowns = post_profile_activity_breakdown['data'][0]['total_value']['breakdowns'][0]['results']
                breakdown_dict = format_action_breakdown_dict(all_breakdowns)
            except KeyError:
                breakdown_dict = {'bio_link_clicked': 0, 'call': 0, 'direction': 0, 'email': 0, 'other': 0, 'text': 0}
            data_dict.update(breakdown_dict)
            media_info.update(data_dict)
            post_list.append(media_info)

        if media_info['media_type'] == 'CAROUSEL_ALBUM':
            url = "https://graph.facebook.com/v16.0/{}/insights?metric=carousel_album_engagement,carousel_album_impressions,carousel_album_reach,carousel_album_saved,carousel_album_video_views&access_token={}".format(media, access_token)
            media_metrics = get_from_api(url, endpoint='data')
            data_dict = format_data_dict(media_metrics)
            media_info.update(data_dict)
            album_list.append(media_info)

        if media_info['media_product_type'] == 'STORY':
            url = "https://graph.facebook.com/v16.0/{}/insights?metric=comments,likes,plays,reach,saved,shares,total_interactions&access_token={}".format(media, access_token)
            media_metrics = get_from_api(url, endpoint='data')
            data_dict = format_data_dict(media_metrics)

            url = "https://graph.facebook.com/v16.0/{}/insights?metric=profile_activity&breakdown=action_type&access_token={}".format(media, access_token)
            story_profile_activity_breakdown = get_from_api(url)
            try:
                all_breakdowns = story_profile_activity_breakdown['data'][0]['total_value']['breakdowns'][0]['results']
                breakdown_dict = format_action_breakdown_dict(all_breakdowns)
            except KeyError:
                breakdown_dict = {'bio_link_clicked': 0, 'call': 0, 'direction': 0, 'email': 0, 'other': 0, 'text': 0}
            data_dict.update(breakdown_dict)

            url = "https://graph.facebook.com/v16.0/{}/insights?metric=navigation&breakdown=story_navigation_action_type&access_token={}".format(media, access_token)
            story_navigation_breakdown = get_from_api(url)
            try:
                all_breakdowns = story_navigation_activity_breakdown['data'][0]['total_value']['breakdowns'][0]['results']
                breakdown_dict = format_action_breakdown_dict(all_breakdowns)
            except KeyError:
                breakdown_dict = {'automatic_forward': 0, 'tap_back': 0, 'tap_exit': 0, 'tap_forward': 0, 'swipe_back': 0, 'swipe_down': 0, 'swipe_forward': 0, 'swipe_up': 0}
            data_dict.update(breakdown_dict)
            media_info.update(data_dict)
            story_list.append(media_info)

        if media_info['media_product_type'] == 'REELS':
            url = "https://graph.facebook.com/v16.0/{}/insights?metric=exits,impressions,reach,replies,taps_forward,taps_back&access_token={}".format(media, access_token)
            media_metrics = get_from_api(url, endpoint='data')
            data_dict = format_data_dict(media_metrics)
            media_info.update(data_dict)
            reels_list.append(media_info)

    # Write lists to DataFrame
    post_df = pd.DataFrame(post_list)
    album_df = pd.DataFrame(album_list)
    story_df = pd.DataFrame(story_list)
    reels_df = pd.DataFrame(reels_list)

    # Write dataframe to csv
    if not post_df.empty:
        post_df.to_csv('ig_post_metrics.csv', encoding='utf-8')
    if not album_df.empty:
        album_df.to_csv('ig_album_metrics.csv', encoding='utf-8')
    if not reels_df.empty:
        reels_df.to_csv('ig_reels_metrics.csv', encoding='utf-8')
    if not story_df.empty:
        story_df.to_csv('ig_story_metrics.csv', encoding='utf-8')

    return "Data has been saved"
