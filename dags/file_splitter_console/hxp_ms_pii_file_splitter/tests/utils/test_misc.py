import pytest

from hxp_ms_pii_file_splitter.utils.misc import (
    parse_filename,
    get_output_filenames,
    add_data_hash,
)


def test_parse_filename():
    input_path = [r"C:\Users\abcd\folder name\filename.csv"]
    output = [["C:", "Users", "abcd", "folder name", "filename.csv"]]

    index = 0
    for path in input_path:
        array = parse_filename(path)
        assert output[index] == array
        index += 1


def test_get_output_filenames():
    output_filename = get_output_filenames("prefix", "abc.csv", "time_stamp")
    assert output_filename == "prefix_time_stamp_abc.csv"


@pytest.mark.usefixtures("dataframe")
def test_add_data_hash(dataframe):
    columns = dataframe.columns.tolist()
    add_data_hash(dataframe)
    columns.append("Data_Hash")
    assert columns == dataframe.columns.tolist()
