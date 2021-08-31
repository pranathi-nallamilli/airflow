import os
from csv import reader

import pytest

from hxp_ms_pii_file_splitter.main import (
    get_required_fields,
    get_nms_fields,
    split_ms_file,
    split_nms_file,
)


@pytest.mark.usefixtures("config_data")
def test_get_required_fields(config_data):
    fields = get_required_fields(config_data[0])
    assert fields == config_data[1]


def test_get_required_fields_none():
    with pytest.raises(TypeError):
        get_required_fields()


def test_get_required_fields_file_not_exists():
    with pytest.raises(FileNotFoundError):
        get_required_fields("wrong_path/file_not_exists.csv")


@pytest.mark.usefixtures("input_file")
@pytest.mark.usefixtures("config_data")
def test_get_nms_fields(input_file, config_data):
    nms_fields = get_nms_fields(input_file[0], config_data[1])

    for field in nms_fields:
        assert field in input_file[1]

    assert len(nms_fields) == len(input_file[1])


@pytest.mark.usefixtures("input_file")
@pytest.mark.usefixtures("config_data")
def test_split_ms_file(input_file, config_data, tmp_path):
    output_file = "output_ms_file.csv"
    output_filepath = tmp_path / "data1" / output_file

    split_ms_file(input_file[0], config_data[1], output_filepath)
    assert os.path.getsize(output_filepath) > 0


@pytest.mark.usefixtures("input_file")
def test_split_nms_file(input_file, tmp_path):
    output_file = "output_nms_file.csv"
    output_filepath = tmp_path / "data1" / output_file

    split_nms_file(input_file[0], input_file[1], output_filepath)
    assert os.path.getsize(output_filepath) > 0


@pytest.mark.usefixtures("input_file")
@pytest.mark.usefixtures("config_data")
def test_deduplication(input_file, config_data, tmp_path):
    output_file = "output_ms_file.csv"
    output_filepath = tmp_path / "data1" / output_file

    split_ms_file(input_file[0], config_data[1], output_filepath)

    with open(output_filepath, "r") as csvfile:
        csv_reader = reader(csvfile)
        next(csv_reader, None)
        data = [tuple(row) for row in csv_reader]
        print(data)

        data_set = set(data)
        print(data_set)

    assert input_file[2] == len(data_set)
