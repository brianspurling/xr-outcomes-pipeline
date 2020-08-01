"""Pipeline code for migrating Local Authority data."""

from conf import conf

import dataUtils

import pandas as pd

SRC_WORKSHEET_NAME = "Local Authorities"
TRG_FILE_NAME = "local_authorities"


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)

    # Data is entered manually, so we perform some basic data
    # validation. Check for duplicate Local Authority entries
    if not df.xr_la_name.is_unique:
        raise ValueError(
            conf.SOURCE_SS_NAME + ' - ' + SRC_WORKSHEET_NAME +
            ' contains duplicated XR LA names')

    # Warn for any that are declared but do not have a date (technically
    # possible - because date is not known - but not ideal)
    df_temp = df.loc[(df.declaration_date == '') & (df.is_declared == 'YES')]
    if df_temp.shape[0] > 0:
        conf.WARNINGS.append('The following LAs have declared a CE but do ' +
                             'not have a declaration date:\n  - ' +
                             ', '.join(df_temp.xr_la_name.values))

    df = df[[
        'code',
        'ons_la_name',
        'xr_la_name',
        'is_declared',
        'declaration_date',
        'target_net_zero_year',
        'source',
        'action_plan']]

    df.declaration_date = pd.to_datetime(df.declaration_date)

    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
