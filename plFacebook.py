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
    
    
    # Getting Facebook Fans
    log.info("Fetching Facebook Fans from Facebook API")
    fb_account_creation='2018-07-18'
    fb_account_creation=datetime.datetime.strptime(fb_account_creation, '%Y-%m-%d')
    today = datetime.date.today()
    since=(today - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    until=(today).strftime("%Y-%m-%d")
    
    url_l = conf.FACEBOOK_API_URL_FB_FANS.format(
        api_id=conf.GLOBAL_FACEBOOK_ID,
        since=since,
        until=until,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    fb_fans_data = pd.DataFrame(columns=['date',  'fb_fans' ])

    # nextPage = True
    while True:
        r_o = requests.get(url_l)
        data_l = r_o.json()
        if data_l['data'] !=[]:
            like_data=data_l['data'][0]['values']
            end_time=like_data[0]['end_time']
            end_time=end_time[:10]
            end_time=datetime.datetime.strptime(end_time, '%Y-%m-%d') 
            for d in like_data:
                
                fb_fans_data = fb_fans_data.append({
                'date' : d['end_time'],
                'fb_fans': d['value']}, ignore_index=True)
            if end_time >= fb_account_creation:
                # nextPage = True
                url_l = data_l['paging']['previous']
            else:
                break
        else:
            break

    #get all post ids
    url_l = "https://graph.facebook.com/v5.0/239675493315233/posts?access_token="+conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL
    fb_post_ids_data = pd.DataFrame(columns=['date',  'post_ids' ])
    log.info("Fetching Facebook Post Ids")

    # nextPage = True
    while True:
        r_o = requests.get(url_l)
        data_l = r_o.json()

        post_data=data_l['data']
        for d in post_data:
            fb_post_ids_data = fb_post_ids_data.append({
            'date' : d['created_time'],
            'post_ids': d['id']}, ignore_index=True)
        if 'next' in data_l['paging']:
            # nextPage = True
            url_l = data_l['paging']['next']
        else:
            break
        
    # Get Like count for all post ids 
    fb_likes_by_date = pd.DataFrame(columns=['id',  'likes_count' ])
    log.info("Fetching Facebook Post Id Total Like Count")
    for ids in fb_post_ids_data['post_ids']:
        
        url_l = "https://graph.facebook.com/"+ids+"?fields=likes.summary(true)&access_token="+conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL
        r_o = requests.get(url_l)
        data_l = r_o.json()
        fb_likes_by_date = fb_likes_by_date.append({
            'id' : ids,
            'likes_count': data_l['likes']['summary']['total_count']}, ignore_index=True)

    log.info("Merging Post id, likes count and dates")
    fb_data_joined = fb_likes_by_date.merge(
            fb_post_ids_data,
            how='outer',
            left_on='id',
            right_on='post_ids')

    log.info("Aggregating likes count and dates")
    fb_data_joined['date'] = pd.to_datetime(fb_data_joined['date'])
    fb_data_joined['date'] = fb_data_joined['date'].dt.date
    fb_data_joined = fb_data_joined.drop(columns=['id'])
    fb_data_joined = fb_data_joined.drop(columns=['post_ids'])
    fb_data_joined = fb_data_joined.groupby(['date'])['likes_count'].sum().reset_index()
    
    # convert fb fan data date column 
    log.info("Transforming date col of fb fan dataframe")
    fb_fans_data['date'] = pd.to_datetime(fb_fans_data['date'])
    fb_fans_data['date'] = fb_fans_data['date'].dt.date
    
    # Merge likes and follows and write to source SS
    log.info("Merging Like and Fan data")
    fb_data_joined = fb_data_joined.merge(
        fb_fans_data,
        how='outer',
        left_on='date',
        right_on='date')
    
    # Renaming columns
    fb_data_joined = fb_data_joined.rename(columns={
        "date": "Date",
        "fb_fans": "Cumulative Follows",
        "likes_count": "Daily Likes"})
    log.info("Writing Data to Facebook WS")

    # Sorting and writing to sheet
    fb_data_joined=fb_data_joined.sort_values(by=['Date'], ascending=False)
    conf.SRC_SS.write(
        wsName=SRC_WORKSHEET_NAME,
        df=fb_data_joined,
        bulk_or_delta='BULK')
    

def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
