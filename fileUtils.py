"""Util functions for reading and writing CSV files."""

from pathlib import Path
import pandas as pd
import os

from conf import conf


def writeCSV(df, dir, filename):
    """Write dataframe to CSV."""
    if not os.path.isdir(Path.cwd() / dir):
        os.mkdir(Path.cwd() / dir)

    if df is not None:

        filePath = Path.cwd() / dir / (filename + '.csv')

        df.to_csv(filePath, index=False)

    return filePath


def readCSV(filename):
    """Read dataframe from CSV."""
    fileName = Path.cwd() / conf.TMP_DATA_DIR / (filename + '.csv')
    df = pd.read_csv(fileName)
    return df
