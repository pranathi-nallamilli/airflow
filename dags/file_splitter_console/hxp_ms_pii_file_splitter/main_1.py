import sys
import time
import logging
from datetime import datetime

import pandas as pd

from hxp_ms_pii_file_splitter.utils.file_handling import get_all_fields
from hxp_ms_pii_file_splitter.utils.misc import (
    get_output_filenames,
    parse_filename,
    add_data_hash,
)


logger = logging.getLogger(__name__)


def begin_logging():
    rootlogger = logging.getLogger(__name__)
    if rootlogger.hasHandlers():
        raise Exception("Root logger already has handlers assigned!")
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    consoleout = logging.StreamHandler(sys.stderr)
    consoleout.setFormatter(formatter)
    rootlogger.addHandler(consoleout)
    rootlogger.setLevel(logging.INFO)


def get_required_fields(filepath):
    try:
        dataframe = pd.read_csv(filepath, usecols=None, dtype=str)
    except FileNotFoundError as error:
        logger.error("Failed to read the csv file! Error : %s", error)
        raise error

    required_fields = [field.upper() for field in dataframe["Required Field"]]
    return sorted(required_fields)


def get_nms_fields(filepath, required_fields):
    try:
        all_fields = get_all_fields(filepath)
    except FileNotFoundError as e:
        logger.error("Failed to read the csv file! Error : %s", e)
        raise e

    return [field for field in all_fields if field not in required_fields]


def split_ms_file(filepath, required_fields, output_filepath):
    try:
        ms_data = pd.read_csv(filepath, usecols=required_fields, dtype=str)[
            required_fields
        ]
    except Exception as error:
        logger.error("Failed to read the csv file! Error : %s", error)
        raise error

    ms_data.drop_duplicates(keep="first", inplace=True)
    add_data_hash(ms_data)

    try:
        ms_data.to_csv(output_filepath, index=False, mode="w", header=True)
    except Exception as error:
        logger.error(
            "Failed to write the output microservices csv file for! Error : %s", error
        )
        raise error


def split_nms_file(filepath, fields, output_filepath):
    try:
        nms_fields_iterator = pd.read_csv(
            filepath, usecols=fields, dtype=str, chunksize=1000000
        )
    except Exception as error:
        logger.error("Failed to read the csv file! Error : %s", error)
        raise error

    header = True
    try:
        for chunk in nms_fields_iterator:
            chunk.to_csv(output_filepath, index=False, mode="a", header=header)
            header = False
    except Exception as error:
        logger.error(
            "Failed to write the output non-microservices csv file! Error : %s", error
        )
        raise error


def main():
    start = time.time()
    begin_logging()

    input_file = sys.argv[1]
    config_file = sys.argv[2]

    parse_name = parse_filename(input_file)
    FOLDER_PATH = "/".join(parse_name[:-1])
    filename = parse_name[-1]

    now = datetime.now()
    timestamp = now.strftime("%d%m%Y-%H%M%S-%f")
    ms_filename = get_output_filenames("ms", filename, timestamp)
    nms_filename = get_output_filenames("nms", filename, timestamp)

    ms_fields = get_required_fields(config_file)

    split_ms_file(input_file, ms_fields, FOLDER_PATH + "/" + ms_filename)

    nms_fields = get_nms_fields(input_file, ms_fields)
    split_nms_file(input_file, nms_fields, FOLDER_PATH + "/" + nms_filename)

    end = time.time()
    logger.info("Total execution time : %s", end - start)
