"""Main pipeline control module."""

import sys

from conf import conf
import logger as log
from gSheet import GSheet

import plLocalAuthorities
import plPoliticalParties
import plWebsite
import plSocialMedia
import plBookSales


def run(args):
    """Run pipeline function specified in command line args."""
    conf.loadConfig(args)

    log.jobStart(conf.FUNCTION_NAME)

    conf.SRC_SS = GSheet(conf.SOURCE_SS_NAME)
    conf.TRG_SS = GSheet(conf.TARGET_SS_NAME)

    try:

        globals()[conf.FUNCTION_NAME]()

    except KeyError:
        raise ValueError('Function ' + conf.FUNCTION_NAME + ' not recognised')

    log.jobEnd(conf.FUNCTION_NAME, conf.WARNINGS)


def pipeline():
    """Run the whole pipeline."""
    # Extract Data

    extractWebStats()

    # Migrate from source to target

    plLocalAuthorities.migrate()

    plPoliticalParties.migrate()

    plWebsite.migrate()

    plSocialMedia.migrate()

    plBookSales.migrate()


def extractWebStats():
    """Run the websites data extract."""
    plWebsite.extract()


if __name__ == "__main__":
    run(sys.argv[1:])
