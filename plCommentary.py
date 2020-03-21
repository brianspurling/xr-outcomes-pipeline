"""Pipeline code for extracting and migrating commentary text."""

from conf import conf
import pandas as pd

import dataUtils

SRC_WORKSHEET_NAME = "Commentary"
TRG_FILE_NAME = "commentary"


def migrate():
    """Migrate data from source to target."""
    df = conf.SRC_SS.read(SRC_WORKSHEET_NAME)
    df['chart_name'] = df['chart_name'].str.strip()
    df['commentary_text'] = df['commentary_text'].str.strip()
    dataUtils.outputReportData(
        df=df,
        fileName=TRG_FILE_NAME,
        writeLocalCSV=conf.LOCAL_CSV_OP)
