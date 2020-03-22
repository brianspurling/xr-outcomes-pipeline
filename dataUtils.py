from conf import conf
import logger as log

import boto3
import s3fs


def outputReportData(df, fileName, writeLocalCSV=False):

    if conf.AWS_ACCESS_KEY_ID == '':
        writeLocalCSV = True

    # Data is written to GSheet for easy review by XR team
    # Can also writes to local CSV for easy review by developer
    conf.TRG_SS.write(
        wsName=fileName,
        df=df,
        bulk_or_delta='BULK',
        copy_to_csv=writeLocalCSV)

    # Data for report uploaded to S3

    if conf.AWS_ACCESS_KEY_ID == '':

        log.info('No S3 creds found; data written to CSV')

    else:

        log.info('Writing data to S3 (' + conf.S3_BUCKET + ')')

        bytes_to_write = df.to_csv(None, index=False).encode()
        fs = s3fs.S3FileSystem(
            key=conf.AWS_ACCESS_KEY_ID,
            secret=conf.AWS_SECRET_ACCESS_KEY)

        with fs.open('s3://' + conf.S3_BUCKET + '/' + fileName + '.csv', 'wb') as f:
            f.write(bytes_to_write)
