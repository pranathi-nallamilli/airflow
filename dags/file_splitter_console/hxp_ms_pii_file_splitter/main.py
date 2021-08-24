from utils.file_handling import *
from utils.misc import *
import sys
import time
import logging
import pandas as pd


logger = logging.getLogger(__name__)


def begin_logging():
    logger = logging.getLogger()
    if logger.hasHandlers():
        raise Exception("Root logger already has handlers assigned!")
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    consoleout = logging.StreamHandler(sys.stderr)
    consoleout.setFormatter(formatter)
    logger.addHandler(consoleout)
    logger.setLevel(logging.INFO)


def get_required_fields(config_file):
    try:
        dataframe = pd.read_csv(config_file, usecols=None, dtype=str)
    except Exception as e:
        logger.error("Failed to read the csv file! Error : "+str(e))
        sys.exit(10)

    ms_fields = [field.upper() for field in dataframe['Required Field']]
    return sorted(ms_fields)


def get_nms_fields(input_file, ms_fields):
    all_fields = get_all_fields(input_file)
    return [field for field in all_fields if field not in ms_fields]


def split_ms_file(input_file, ms_fields, output_filepath):
    try:
        ms_data = pd.read_csv(input_file, usecols=ms_fields, dtype=str)[ms_fields]
    except Exception as e:
        logger.error("Failed to read the csv file! Error : "+str(e))
        sys.exit(10)

    ms_data.drop_duplicates(keep="first", inplace=True)
    add_data_hash(ms_data)

    try:
        ms_data.to_csv(output_filepath, index=False, mode='w', header=True)
    except Exception as e:
        logger.error("Failed to write the output microservices csv file for! Error : "+str(e))
        sys.exit(10)


def split_nms_file(input_file, nms_fields, output_filepath):
    try:
        nms_fields_iterator = pd.read_csv(input_file, usecols=nms_fields, dtype=str, chunksize=1000000)
    except Exception as e:
        logger.error("Failed to read the csv file! Error : "+str(e))
        sys.exit(10)

    header = True
    try:
        for chunk in nms_fields_iterator:
            chunk.to_csv(output_filepath, index=False, mode='a', header=header)
            header = False
    except Exception as e:
        logger.error("Failed to write the output non-microservices csv file! Error : "+str(e))
        sys.exit(10)


if __name__ == '__main__':
    start = time.time()
    begin_logging()

    input_file = sys.argv[1]
    config_file = sys.argv[2]

    parse_name = parse_filename(input_file)
    folder_path = '/'.join(parse_name[:-1])
    filename = parse_name[-1]

    output_filenames = get_output_filenames(filename)

    ms_fields = get_required_fields(config_file)

    split_ms_file(input_file, ms_fields, folder_path + '/' + output_filenames["ms_filename"])

    nms_fields = get_nms_fields(input_file, ms_fields)
    split_nms_file(input_file, nms_fields, folder_path + '/' + output_filenames["nms_filename"])

    end = time.time()
    logger.info("Total execution time : {}".format(end-start))
