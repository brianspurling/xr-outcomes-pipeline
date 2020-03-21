"""Pipeline code for extracting & migrating web stat data."""

from conf import conf

import dataUtils
import logger as log

import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build as google_build
import os
import json

SRC_WORKSHEET_NAME = "Website"
TRG_FILE_NAME = "website"


def extract():
    """Process JSON response from API."""
    log.info("Fetching website data from Google Analytics API")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            conf.GOOGLE_API_KEY_FILE)
    except IOError:
        service_account_info = \
            json.loads(os.environ.get('GOOGLE_CREDENTIALS_JSON'))
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info)

    scoped_credentials = credentials.with_scopes(
        scopes=conf.GOOGLE_API_SCOPE)

    analytics = google_build(
        'analyticsreporting',
        'v4',
        credentials=scoped_credentials)

    df = pd.DataFrame()
    for view in conf.GOOGLE_ANALYTICS_VIEWS:
        # Dimensions and Metrics list:
        # https://www.ovrdrv.com/ultimate-google-analytics-dimensions-and-metrics-list/
        res = analytics.reports().batchGet(
            body={
                'reportRequests': [{
                    'viewId': view['view_id'],
                    'dateRanges': [{
                        'startDate': '2019-01-01',
                        'endDate': 'yesterday'}],
                    'metrics': [
                        {'expression': 'ga:pageviews'},
                        {'expression': 'ga:sessions'}],
                    'includeEmptyRows': True,
                    'dimensions': [
                        {'name': 'ga:date'}]}]}).execute()

        df_v = processResponse(res)

        # The domain URL is held in our conf object, which we're iterating
        # through. Write it to the dataframe.
        df_v['domain'] = view['domain']

        df = df.append(df_v)

    df['ga:date'] = pd.to_datetime(
        df['ga:date'],
        format='%Y%m%d',
        utc=False,
        errors='coerce').dt.date

    conf.SRC_SS.write(
        wsName=SRC_WORKSHEET_NAME,
        df=df,
        bulk_or_delta='BULK')


def processResponse(response):
    """Process JSON response from API."""
    data = []
    for report in response.get('reports', []):

        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = \
            columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dataRow = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                dataRow[header] = dimension

            for i, values in enumerate(dateRangeValues):
                vs = values.get('values')
                for metricHeader, value in zip(metricHeaders, vs):
                    dataRow[metricHeader.get('name')] = value
            data.append(dataRow)
    df = pd.DataFrame(data)
    return(df)


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
