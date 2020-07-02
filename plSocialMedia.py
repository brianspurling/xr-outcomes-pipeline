"""Pipeline code for extracting & migrating social media data."""

from conf import conf

import dataUtils

import pandas as pd

SRC_WORKSHEET_NAME = "Social Media"
TRG_FILE_NAME = "social_media"


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    # Data is entered manually, so we perform some basic data
    # validation

    # Remove any that don't have a date
    df = df.loc[~pd.isnull(df.date)]

    df.loc[df.platform == 'YouTube', 'follows_cum'] = \
        df.loc[df.platform == 'YouTube', 'follows'].cumsum()
        
    # There are some gaps in the cumulative source data which we plug
    # manually to smooth out the charts
    mask = df.platform == 'Twitter'
    mask = mask & (df.account_id == 'ExtinctionR')
    mask = mask & (df.date == '2019-04-20')
    df.loc[mask, 'follows_cum'] = 118437

    mask = df.platform == 'Facebook'
    mask = mask & (df.account_id == 'XRebellionUK')
    mask = mask & (df.date == '2019-10-06')
    df.loc[mask, 'follows_cum'] = 18942

    df.date = pd.to_datetime(df.date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
