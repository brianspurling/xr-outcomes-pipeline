"""Load CLI variables."""

FUNC_NAMES = {
    'pipeline': "Pipeline execution"}

ARG_HELP_TXT_FUNCTION_NAME = \
    "Name of the function you want to run. Specify one of: " + str(FUNC_NAMES)
ARG_HELP_TXT_READ_FROM_FILE = \
    "Where possible, read data from previously loaded CSVs " + \
    "to speed up execution"
ARG_HELP_TXT_LOCAL_CSV_OP = \
    "Create local CSV output files as well as uploading."
