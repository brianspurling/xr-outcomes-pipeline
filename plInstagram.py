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

    # Getting Instagram likes

    log.info("Fetching Instagram likes from Facebook API")

    url_l = conf.FACEBOOK_API_URL_LIKES.format(
        api_id=conf.GLOBAL_INSTAGRAM_ID,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    instaLikesData = []
    while True:
        r_o = requests.get(url_l)
        data_l = r_o.json()
        like_data = data_l['data']
        for d in like_data:
            instaLike = {
                'date': d['timestamp'],
                'permalink': d['permalink'],
                'insta_likes_count': d['like_count']}
            instaLikesData.append(instaLike)
        if 'next' in data_l['paging']:
            url_l = data_l['paging']['next']
        else:
            break

    df_instaLikes = pd.DataFrame(instaLikesData)
    df_instaLikes['time'] = pd.to_datetime(df_instaLikes['date'])
    df_instaLikes['date'] = df_instaLikes['time'].dt.date
    sum_insta_likes_data = df_instaLikes.groupby(['date'])['insta_likes_count'].sum()

    # Getting Instagram followers

    log.info("Fetching Instagram follows from Facebook API")

    today = datetime.date.today()
    since=(today - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    until=(today).strftime("%Y-%m-%d")

    url_o = conf.FACEBOOK_API_URL_FOLLOWS.format(
        api_id=conf.GLOBAL_INSTAGRAM_ID,
        since=since,
        until=until,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    instaFollowsData = []
    x = 1
    for x in range(1, 150):
        r_o = requests.get(url_o)
        data_o = r_o.json()
        follow_data=data_o['data'][0]['values']
        for d in follow_data:
            instaFollow = {
                 'date': d['end_time'],
                 'insta_follower_count': d['value']}
            instaFollowsData.append(instaFollow)
        url_o = data_o['paging']['previous']
        x += 1

    df_instaFollows = pd.DataFrame(instaFollowsData)
    df_instaFollows['date'] = pd.to_datetime(df_instaFollows['date'])
    df_instaFollows['date'] = df_instaFollows['date'].dt.date

    # Merge likes and follows and write to source SS

    insta_data_joined = df_instaFollows.merge(
        sum_insta_likes_data,
        how='outer',
        left_on='date',
        right_on='date')

    insta_data_joined = insta_data_joined[[
        'date',
        'insta_likes_count',
        'insta_follower_count']]

    insta_data_joined = insta_data_joined.sort_values('date')
    insta_data_joined['cumulative_followers'] = \
        insta_data_joined.insta_follower_count.cumsum()

    insta_data_joined = insta_data_joined.rename(columns={
        "date": "Date",
        "insta_likes_count": "Daily Likes",
        "insta_follower_count": "New Daily Followers",
        "cumulative_followers": "Cumulative Followers"})

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
