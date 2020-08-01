"""Pipeline code for extracting & migrating Instagram data"""
from conf import conf
import dataUtils
import time
import datetime
import logger as log
import pandas as pd
import requests
import os
import json

SRC_WORKSHEET_NAME = "Instagram"
TRG_FILE_NAME = "instagram"

def extract():

    # Getting Instagram followers

    log.info("Fetching Instagram follows from Facebook API")

    df_instaFollows = dataUtils.facebookGraphAPI_getFollows(
        appID=conf.GLOBAL_INSTAGRAM_ID,
        dayRange=conf.FACEBOOK_API_IG_DAY_RANGE,
        metric='follower_count',
        accountStartDate=conf.GLOBAL_INSTAGRAM_ACCOUNT_CREATION_DATE)

    # Getting Instagram likes

    log.info("Fetching Instagram likes from Facebook API")

    url = conf.FACEBOOK_API_URL_LIKES.format(
        api_id=conf.GLOBAL_INSTAGRAM_ID,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    likes = []
    while url:
        r = dataUtils.requestGet(url, 10)
        url = None
        rawData = r.json()
        rawLikes = rawData['data']
        for rawLike in rawLikes:
            likes.append({
                'date': rawLike['timestamp'],
                'insta_likes_count': rawLike['like_count']})
        if 'next' in rawData['paging']:
            url = rawData['paging']['next']

    df_instaLikes = pd.DataFrame(likes)
    df_instaLikes['time'] = pd.to_datetime(df_instaLikes['date'])
    df_instaLikes['date'] = df_instaLikes['time'].dt.date
    sum_insta_likes_data = df_instaLikes.groupby(['date'])['insta_likes_count'].sum()

    # Merge likes and follows and write to source SS

    insta_data_joined = df_instaFollows.merge(
        sum_insta_likes_data,
        how='outer',
        left_on='date',
        right_on='date')

    insta_data_joined = insta_data_joined[[
        'date',
        'insta_likes_count',
        'follows']]

    insta_data_joined = insta_data_joined.sort_values('date')

    # Unlke facebook, the follows data from Insta comes back as daily figures
    insta_data_joined.follows = insta_data_joined.follows.cumsum()

    insta_data_joined = insta_data_joined.rename(columns={
        "date": "Date",
        "follows": "Cumulative Follows",
        "insta_likes_count": "Daily Likes"})

    mask = insta_data_joined['Date'] >= conf.GLOBAL_FACEBOOK_ACCOUNT_CREATION_DATE.date()
    insta_data_joined = insta_data_joined.loc[mask]
    conf.SRC_SS.write(
        wsName="Instagram",
        df=insta_data_joined,
        bulk_or_delta='BULK')


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
