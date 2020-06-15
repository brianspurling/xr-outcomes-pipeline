"""Pipeline code for extracting & migrating Action Network data."""

from conf import conf

import dataUtils
import logger as log

import pandas as pd

import requests
import io

import os
import json

SRC_WORKSHEET_NAME = "Action Network"
TRG_FILE_NAME = "action_network"


def extract():
    """Process csv file from API."""
    log.info("Fetching Action Network data from NextCloud API")

    r = requests.get(
        url=conf.ACTION_NETWORK_NEXTCLOUD_FILE_URL,
        auth=(conf.NEXTCLOUD_USER_NAME, conf.NEXTCLOUD_PASSWORD)
    )

    df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

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
