import hashlib
from datetime import datetime


def get_output_filenames(input_filename):
    now = datetime.now()
    timestamp = now.strftime("%d%m%Y-%H%M%S-%f")
    output_filenames = {"ms_filename": 'ms_{0}_{1}'.format(timestamp, input_filename),
                        "nms_filename": 'nms_{0}_{1}'.format(timestamp,input_filename)}
    return output_filenames


def parse_filename(filename):
    return filename.replace('\\', '/').split('/')


def create_hash(column_list):
    column_list_str = map(str, column_list)
    return hashlib.md5(''.join(column_list_str).encode()).hexdigest()


def add_data_hash(dataframe):
    dataframe["Data_Hash"] = dataframe.apply(create_hash, axis=1)
