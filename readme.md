# Project Title

An data pipeline to extract and prep the data for Extinction Rebellion's UK Outcomes dashboard (https://github.com/brianspurling/xr-outcomes-dashboard)

## Overview

The application consists of a set of Python modules, controlled by main.py, making heavy use of Pandas to read, process and write data.

It is hosted on Heroku, where it is scheduled to run once a day.

The pipeline picks up data from APIs and the SOURCE GSheet and writes it to the TARGET GSheet (for visibility) and S3 (for the dashboard)

If no S3 creds are supplied, it creates CSV files instead.

## Running Locally

Prerequisites: Python v3, virtualenv, pip

- Clone this repo

1. Set up a virtualenv
```
$ cd xr-outcomes-pipeline
$ virtualenv ~/venvs/xr-outcomes-pipeline
$ source ~/venvs/xr-outcomes-pipeline/bin/activate
$ pip install -r requirements.txt
```

2. Config

Create a config.ini file, containing two values: `SOURCE_SS_NAME` and `TARGET_SS_NAME`

Get the Google Auth creds file and save as google_auth_prod_creds.json

3. Running pipeline locally
```
$ python main.py
```

CSVs will be written to `../data`, where the xr-outcomes-dashboard will expect to find them (if run locally)

## Deploying Changes to Staging

1. Ask to be added as sa contributer to the staging Heroku

2. Add the staging Heroku as a remote repo
```
$ heroku git:remote -a xr-outcomes-pipeline-stage
$ git remote rename heroku heroku-stage
```

3. Deploy new code to staging
```
$ git push heroku-stage [wip-branch:]master
```

## Built With

* [Python](https://www.python.org/)
* [Pandas](https://pandas.pydata.org/) - For data processing
* [GSpread](https://github.com/burnash/gspread) - For pipeline i/o with Google Sheets

## License

TBD

## Acknowledgments

TBD
