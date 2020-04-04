"""Pipeline code for extracting & migrating Action Network data."""

from conf import conf

import dataUtils
import logger as log

import pandas as pd

import os
import json

SRC_WORKSHEET_NAME = "Action Network"
TRG_FILE_NAME = "action_network"


def extract():
    """Process JSON response from API."""
    pass
    # log.info("Fetching website data from Action Network API")


def migrate():
    """Migrate data from source to target."""
    pass
    # df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    # dataUtils.outputReportData(
    #     df=df,
    #     fileName=TRG_FILE_NAME,
    #     writeLocalCSV=conf.LOCAL_CSV_OP)
