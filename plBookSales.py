"""Pipeline code for extracting and migrating book sales data."""

from conf import conf
import dataUtils

import pandas as pd

SRC_WORKSHEET_NAME = "Book Sales"
TRG_FILE_NAME = "book_sales"


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    # Data is entered manually, so we perform some basic data
    # validation

    # Remove any that don't have an as-at date
    df = df.loc[~pd.isnull(df.date)]

    # Remove any that don't have a sales number
    df = df.loc[~pd.isnull(df.sales)]

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
