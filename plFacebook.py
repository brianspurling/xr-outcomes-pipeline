"""Pipeline code for extracting & migrating Facebook data"""
from conf import conf
import dataUtils
import time
import datetime
import logger as log
import pandas as pd
import requests
import os
import json

SRC_WORKSHEET_NAME = "Facebook"
TRG_FILE_NAME = "facebook"

def extract():

    # Get Facebook follows

    log.info("Fetching Facebook follows from Facebook API")

    df_fbFollows = dataUtils.facebookGraphAPI_getFollows(
        appID=conf.GLOBAL_FACEBOOK_ID,
        dayRange=conf.FACEBOOK_API_FB_DAY_RANGE,
        metric='page_fans', # Facebook only allows "fans", not followers (which Insta does allow)
        accountStartDate=conf.GLOBAL_FACEBOOK_ACCOUNT_CREATION_DATE)

    # Get post ids

    log.info("Fetching Facebook post IDs from Facebook API")

    url = conf.FACEBOOK_API_URL_FB_POSTS.format(
        api_id=conf.GLOBAL_FACEBOOK_ID,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    posts = []
    while url:
        r = dataUtils.requestGet(url, 10)
        url = None
        rawData = r.json()
        rawPosts = rawData['data']
        for rawPost in rawPosts:
            posts.append({
                'date' : rawPost['created_time'],
                'post_id': rawPost['id']})
        if 'next' in rawData['paging']:
            url = rawData['paging']['next']

    df_posts = pd.DataFrame(posts)

    # Get likes for all post ids

    log.info("Fetching Facebook posts' likes from Facebook API")

    likes = []
    for i, row in df_posts.iterrows():
        url = conf.FACEBOOK_API_URL_FB_LIKES.format(
            post_id=row['post_id'],
            api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)
        r = dataUtils.requestGet(url, 10)
        data = r.json()
        likes.append({
            'post_id' : row['post_id'],
            'likes_count': data['likes']['summary']['total_count']})

    df_fbLikes = pd.DataFrame(likes)

    # Process Data

    # Merge Post id, likes count and dates
    df_fbPostsAndLikes = df_fbLikes.merge(
            df_posts,
            how='outer',
            left_on='post_id',
            right_on='post_id')

    # Aggregate likes count and dates
    df_fbPostsAndLikes['date'] = pd.to_datetime(df_fbPostsAndLikes['date'])
    df_fbPostsAndLikes['date'] = df_fbPostsAndLikes['date'].dt.date
    df_fbPostsAndLikes = df_fbPostsAndLikes.drop(columns=['post_id'])
    df_fbPostsAndLikes = df_fbPostsAndLikes.groupby(['date'])['likes_count'].sum().reset_index()

    # Merge likes and follows
    df_fbAll = df_fbPostsAndLikes.merge(
        df_fbFollows,
        how='outer',
        left_on='date',
        right_on='date')

    # Could be no likes on a given day
    df_fbAll.likes_count = df_fbAll.likes_count.fillna(0)

    # Renaming columns
    df_fbAll = df_fbAll.rename(columns={
        "date": "Date",
        "follows": "Cumulative Follows",
        "likes_count": "Daily Likes"})

    # Sorting and writing to sheet

    df_fbAll = df_fbAll.sort_values(by=['Date'], ascending=False)

    # Remove today's data, because Facebook doesn't give us cumulative follows
    # for the latest date
    df_fbAll = df_fbAll.drop(df_fbAll.index[0])

    conf.SRC_SS.write(
        wsName=SRC_WORKSHEET_NAME,
        df=df_fbAll,
        bulk_or_delta='BULK')


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
