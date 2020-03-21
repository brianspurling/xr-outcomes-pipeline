"""Pipeline code for migrating Net Zero data."""
from conf import conf

import dataUtils

import pandas as pd

SRC_WORKSHEET_NAME = "Political Parties"
TRG_FILE_NAME = "political_parties"


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    # Data is entered manually, so we perform some basic data
    # validation

    # Check for duplicate Organiation Names
    if not df.org_name.is_unique:
        raise ValueError(
            conf.SOURCE_SS_NAME + ' - ' + SRC_WORKSHEET_NAME +
            ' contains duplicated Org names')

    # Remove any that don't have a target year
    df = df.loc[~pd.isnull(df.target_net_zero_year)]

    df.date_call_made = pd.to_datetime(df.date_call_made)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
