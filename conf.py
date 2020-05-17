"""Processes and stores config.

Calls Conf class constructor immediately, ensuring
same conf object is available for import to all other
modules
"""

from configobj import ConfigObj
import argparse
import cli
import configVars
import os


class Conf():
    """Class to process and hold all config variables."""

    def __init__(self):
        """Constructor: does nothing because we init object in main.py."""
        pass

    def loadConfig(self, args):
        """Load config from file / command line args into object.

        Processes config values from configVars.py, config.ini, and
        cmd line args.
        """
        self.confDict = dict(
            configVars.CONFIG_VARS,
            **ConfigObj('config.ini'),
            **processArgs(args))

        for var in self.confDict:
            setattr(Conf, var, self.confDict[var])

        # We get our S3 creds from 1. environment variables, 2. config.ini
        # If neither, set to None (and app will attempt to load from CSV)
        if 'AWS_ACCESS_KEY_ID' not in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ or 'S3_BUCKET' not in os.environ:
            if hasattr(self, 'AWS_ACCESS_KEY_ID') and hasattr(self, 'AWS_SECRET_ACCESS_KEY') and hasattr(self, 'S3_BUCKET'):
                os.environ["AWS_ACCESS_KEY_ID"] = self.AWS_ACCESS_KEY_ID
                os.environ["AWS_SECRET_ACCESS_KEY"] = self.AWS_SECRET_ACCESS_KEY
                os.environ["S3_BUCKET"] = self.S3_BUCKET

            else:
                os.environ["AWS_ACCESS_KEY_ID"] = ''
                os.environ["AWS_SECRET_ACCESS_KEY"] = ''
                os.environ["S3_BUCKET"] = ''
        self.AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
        self.AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.S3_BUCKET = os.environ["S3_BUCKET"]

        # Similar set up for spreadsheet names
        if 'SOURCE_SS_NAME' not in os.environ or 'TARGET_SS_NAME' not in os.environ:
            if hasattr(self, 'SOURCE_SS_NAME') and hasattr(self, 'TARGET_SS_NAME'):
                os.environ["SOURCE_SS_NAME"] = self.SOURCE_SS_NAME
                os.environ["TARGET_SS_NAME"] = self.TARGET_SS_NAME
            else:
                raise ValueError("Missing config: source or target spreadsheet names")
        self.SOURCE_SS_NAME = os.environ["SOURCE_SS_NAME"]
        self.TARGET_SS_NAME = os.environ["TARGET_SS_NAME"]

        # And API keys

        if 'ACTION_NETWORK_API_KEY' not in os.environ:
            if hasattr(self, 'ACTION_NETWORK_API_KEY'):
                os.environ["ACTION_NETWORK_API_KEY"] = self.ACTION_NETWORK_API_KEY
            else:
                raise ValueError("Missing config: Action Network API Key")
        self.ACTION_NETWORK_API_KEY = os.environ["ACTION_NETWORK_API_KEY"]

        if 'FACEBOOK_INSTAGRAM_API_KEY_GLOBAL' not in os.environ:
            if hasattr(self, 'FACEBOOK_INSTAGRAM_API_KEY_GLOBAL'):
                os.environ["FACEBOOK_INSTAGRAM_API_KEY_GLOBAL"] = self.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL
            else:
                raise ValueError("Missing config: Facebook/Instagram (global) API Key")
        self.FACEBOOK_INSTAGRAM_API_KEY_GLOBAL = os.environ["FACEBOOK_INSTAGRAM_API_KEY_GLOBAL"]

        self.WARNINGS = []
        self.SRC_SS = None
        self.TRG_SS = None


# We instantiate the class immediately so all
# modules can import the same object

conf = Conf()


def processArgs(args):
    """Process command line args."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-f',
        '--function_name',
        help=cli.ARG_HELP_TXT_FUNCTION_NAME)

    parser.add_argument(
        '--file',
        help=cli.ARG_HELP_TXT_READ_FROM_FILE,
        action='store_true')

    parser.add_argument(
        '--local_csv',
        help=cli.ARG_HELP_TXT_LOCAL_CSV_OP,
        action='store_true')

    argsRaw = parser.parse_args()

    # Set default args, then edit based on command line args
    args = {
        'FUNCTION_NAME': 'pipeline',
        'READ_FROM_FILE': False,
        'LOCAL_CSV_OP': False}

    if argsRaw.function_name:
        args['FUNCTION_NAME'] = argsRaw.function_name
    if argsRaw.file:
        args['READ_FROM_FILE'] = True
    if argsRaw.local_csv:
        args['LOCAL_CSV_OP'] = True

    return args
