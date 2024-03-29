"""Util functions for reading and writing GSheet files."""

import gspread
import gspread_dataframe as gspread_df
import json
from authlib.integrations.requests_client import AssertionSession
import pandas as pd
import os

from conf import conf
import fileUtils


class GSheet():
    """GSheet class enables interaction with a Google Spreadsheet."""

    def __init__(self, ssName):
        """Set up connection to GSheet."""
        self.ssName = ssName

        self.log_connecting()

        try:
            with open(conf.GOOGLE_API_KEY_FILE, 'r') as f:
                credentials = json.load(f)
        except IOError:
            credentials = json.loads(os.environ.get('GOOGLE_CREDENTIALS_JSON'))

        self.client = gspread.service_account_from_dict(credentials)

        self.ss = self.client.open(ssName)

        self.log_connected()


    def read(self, wsName):
        """Read data from GSheet."""
        self.log_read_start(wsName)

        if conf.READ_FROM_FILE:

            fileName = (conf.TMP_DATA_DIR +
                        '/gsheet-' + self.ssName + '.' + wsName + '.csv')
            df = pd.read_csv(fileName)

        else:

            ws = self.ss.worksheet(wsName)

            rawData = ws.get_all_values()
            if len(rawData) > 0:
                df = pd.DataFrame(rawData[1:], columns=rawData[0])
            else:
                df = pd.DataFrame()

        # To encourage human-friendly column names in the source GSheet, we
        # hold a column mapping ("source to target" mapping) in config
        # We also lose excess columns at this point.
        try:
            cols = conf.STM[wsName]
        except KeyError:
            raise KeyError('Could not find ' + str(wsName) + ' in the STM ' +
                           '(configVars). Add the worksheet, with all ' + 
                           'source columns you want mapped over to target ' +
                           'columns')

        df = df.rename(columns=cols)
        df = df[conf.STM[wsName].values()]

        # Data tends to be read into Pandas as strings, so we set data types of
        # columns based on lists of column names in conf

        for dateColName in conf.DATE_COLUMNS:
            if dateColName in list(df):
                df[dateColName] = \
                    pd.to_datetime(
                        df[dateColName],
                        dayfirst=True,
                        utc=False,
                        errors='coerce')

        for floatColName in conf.FLOAT_COLUMNS:
            if floatColName in list(df):
                df[floatColName] = \
                    pd.to_numeric(
                        df[floatColName],
                        downcast='float',
                        errors='coerce')

        for intColName in conf.INT_COLUMNS:
            if intColName in list(df):
                df[intColName] = \
                    pd.to_numeric(
                        df[intColName],
                        downcast='integer',
                        errors='coerce')

        self.log_read_end(df.shape)

        # We write to CSV so we can pick it up from file next time (FASTER!)

        fileUtils.writeCSV(
            df=df,
            dir=conf.TMP_DATA_DIR,
            filename='gsheet-' + self.ssName + '.' + wsName)

        return df

    def write(self, wsName, df, bulk_or_delta, copy_to_csv=False):
        """Write data to GSheet."""
        if bulk_or_delta is None or bulk_or_delta not in ['BULK', 'DELTA']:
            raise ValueError("bulk_or_delta parameter must be 'BULK' or " +
                             "'DELTA'. You passed " + str(bulk_or_delta))

        if df.shape[0] == 0:
            print("No data to write")
            return

        self.log_write_start(wsName, df.shape)

        ws = self.ss.worksheet(wsName)

        if bulk_or_delta == 'BULK':

            gspread_df.set_with_dataframe(
                worksheet=ws,
                dataframe=df,
                resize=True)

        elif bulk_or_delta == 'DELTA':

            raise ValueError('Delta write not yet implemented!')

        if copy_to_csv:
            fileUtils.writeCSV(
                df=df,
                dir=conf.OP_DATA_DIR,
                filename=wsName)

        self.log_write_end()

    def log_connecting(self):
        """Output "connecting" log msg."""
        op = ''
        op += 'Connecting to GSheet "' + self.ssName + '"... '
        print(op, end='')

    def log_connected(self):
        """Output "connected" log msg."""
        op = 'Connected'
        print(op, end='\n')

    def log_read_start(self, wsName):
        """Output "read start" log msg."""
        op = ''
        op += 'Reading data from "' + wsName + '" worksheet of the '
        op += '"' + self.ssName + '"' + " GSheet... "
        print(op, end='')

    def log_read_end(self, shape):
        """Output "read end" log msg."""
        op = 'Read ' + str(shape[0]) + ' rows and '
        op += str(shape[1]) + ' columns'
        print(op, end='\n')

    def log_write_start(self, wsName, shape):
        """Output "write start" log msg."""
        op = ''
        op += 'Writing ' + str(shape[0]) + ' rows, ' + str(shape[1])
        op += ' cols to worksheet "' + wsName + '" of the '
        op += '"' + self.ssName + '"' + ' GSheet... '
        print(op, end='')

    def log_write_end(self):
        """Output "write end" log msg."""
        op = 'Data written'
        print(op, end='\n')
