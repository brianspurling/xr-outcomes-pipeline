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

    insta_likes_data = pd.DataFrame(columns=['date', 'permalink', 'insta_likes_count'])

    while True:
        r_o = requests.get(url_l)
        data_l = r_o.json()
        like_data=data_l['data']
        for d in like_data:
            insta_likes_data = insta_likes_data.append({
            'date' : d['timestamp'],
            'permalink' : d['permalink'],
            'insta_likes_count': d['like_count']}, ignore_index=True)
        if 'next' in data_l['paging']:
            url_l = data_l['paging']['next']
        else:
            break

    insta_likes_data['time'] = pd.to_datetime(insta_likes_data['date'])
    insta_likes_data['dates'] = insta_likes_data['time'].dt.date
    sum_insta_likes_data=insta_likes_data.groupby(['dates'])['insta_likes_count'].sum()

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

    insta_follow_data = pd.DataFrame(columns=['date','insta_follower_count'])

    x = 1
    for x in range(1, 150):
        r_o = requests.get(url_o)
        data_o = r_o.json()
        follow_data=data_o['data'][0]['values']
        for d in follow_data:
            insta_follow_data = insta_follow_data.append({
                'date' : d['end_time'],
                'insta_follower_count': d['value']}, ignore_index=True)
        url_o = data_o['paging']['previous']
        x += 1

    insta_follow_data['dates'] = pd.to_datetime(insta_follow_data['date'])
    insta_follow_data['dates'] = insta_follow_data['dates'].dt.date
    insta_follow_data = insta_follow_data.drop(columns=['date'])

    # Merge likes and follows and write to source SS

    insta_data_joined = insta_follow_data.merge(
        sum_insta_likes_data,
        how='outer',
        left_on='dates',
        right_on='dates')

    insta_data_joined = insta_data_joined.rename(columns={
        "dates": "Date",
        "insta_likes_count": "Daily Likes",
        "insta_follower_count": "New Daily Followers"})

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
