from conf import conf
import logger as log

import boto3
import s3fs
import datetime
import requests
import os
import json
import pandas as pd


def outputReportData(df, fileName, writeLocalCSV=False):

    if conf.AWS_ACCESS_KEY_ID == '':
        writeLocalCSV = True

    # Data is written to GSheet for easy review by XR team
    # Can also writes to local CSV for easy review by developer
    conf.TRG_SS.write(
        wsName=fileName,
        df=df,
        bulk_or_delta='BULK',
        copy_to_csv=writeLocalCSV)

    # Data for report uploaded to S3

    if conf.AWS_ACCESS_KEY_ID == '':

        log.info('No S3 creds found; data written to CSV')

    else:

        log.info('Writing data to S3 (' + conf.S3_BUCKET + ')')

        bytes_to_write = df.to_csv(None, index=False).encode()
        fs = s3fs.S3FileSystem(
            key=conf.AWS_ACCESS_KEY_ID,
            secret=conf.AWS_SECRET_ACCESS_KEY)

        with fs.open('s3://' + conf.S3_BUCKET + '/' + fileName + '.csv', 'wb') as f:
            f.write(bytes_to_write)


def facebookGraphAPI_getFollows(dayRange, metric, accountStartDate):

    today = datetime.date.today()
    since = (today - datetime.timedelta(days=dayRange)).strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")

    url = conf.FACEBOOK_API_URL_FOLLOWS.format(
        api_id=conf.GLOBAL_FACEBOOK_ID,
        metric='page_fans', # Facebook only allows "fans", not followers (which Insta does allow)
        since=since,
        until=until,
        api_key=conf.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL)

    follows = []
    while url:
        r = requestGet(url, 10)
        url = None
        rawData = r.json()
        # The last call might be beyond the account start date and thus return
        # no data
        if rawData['data'] != []:
            rawFollows = rawData['data'][0]['values']
            for rawFollow in rawFollows:
                follows.append({
                    'date' : rawFollow['end_time'],
                    'follows': rawFollow['value']})
            # The first page returned is the latest data, and we page backwards.
            # The data within a response is returned in date-ascending,
            # so the first value is the earliest date. When this date has passed
            # our account "start date", we can stop looping
            end_time = datetime.datetime.strptime(
                rawFollows[0]['end_time'][:10], '%Y-%m-%d')
            if 'previous' in rawData['paging'] and end_time > accountStartDate:
                url = rawData['paging']['previous']

    df_follows = pd.DataFrame(follows)
    df_follows['date'] = pd.to_datetime(df_follows['date'])
    df_follows['date'] = df_follows['date'].dt.date

    return df_follows


# We get a quite a few connection resets, so in these cases, we'll try again
# up to 10 times
def requestGet(url, retries):
    i = 0
    while i <= retries:
        try:
            r = requests.get(url)
            break
        except requests.exceptions.ConnectionError as err:
            if i < retries:
                i += 1
            else:
                raise err
    return r
