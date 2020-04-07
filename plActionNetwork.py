"""Pipeline code for extracting & migrating Action Network data."""

from conf import conf

import dataUtils
import logger as log

import pandas as pd

import requests
import os
import json

SRC_WORKSHEET_NAME = "Action Network"
TRG_FILE_NAME = "action_network"


def extract():
    """Process JSON response from API."""

    # Until we have creds, we're picking up sample data from file

    log.info("Fetching website data from Action Network API")

    url = conf.ACTION_NETWORK_API_URL
    key = conf.ACTION_NETWORK_API_KEY

    createdDates = []
    activists = []

    nextPage = True
    while nextPage:

        print('Fetching: ' + str(url))
        r = requests.get(url, headers={'OSDI-API-Token': key})
        rawData = r.json()

        if 'next' in rawData['_links']:
            nextPage = True
            url = rawData['_links']['next']['href']
        else:
            # "A next link will be present if the page in the collection is the
            # last page, but not present if no resources are returned (ie.
            # you've gone past the last page), to improve speed."
            break

        for rawPersonData in rawData['_embedded']['osdi:people']:
            createdDates.append(rawPersonData['created_date'])
            activists.append(1)

    df = pd.DataFrame({'Date': createdDates, 'Activists': activists})

    df['Date'] = pd.to_datetime(
        df['Date'],
        utc=False,
        errors='coerce').dt.date

    df = df.groupby(['Date']).agg({'Activists': 'sum'}).reset_index()
    df = df.sort_values('Date')

    conf.SRC_SS.write(
        wsName=SRC_WORKSHEET_NAME,
        df=df,
        bulk_or_delta='BULK')


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
