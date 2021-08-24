from csv import DictWriter

import pandas as pd
import pytest


@pytest.fixture
def config_data(tmp_path):
    csv_columns = ["Microservice Name", "Required Field"]
    data = [
        {"Microservice Name": "MS1", "Required Field": "RF1"},
        {"Microservice Name": "MS1", "Required Field": "RF2"},
        {"Microservice Name": "MS2", "Required Field": "RF3"},
        {"Microservice Name": "MS2", "Required Field": "RF4"},
        {"Microservice Name": "MS3", "Required Field": "RF5"},
        {"Microservice Name": "MS4", "Required Field": "RF6"},
    ]

    required_fields = [row["Required Field"] for row in data]
    csv_file = "config.csv"
    d = tmp_path / "data"
    d.mkdir()
    p = d / csv_file
    with open(str(p), "w") as csvfile:
        writer = DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return str(p), sorted(required_fields)


@pytest.fixture
def input_file(tmp_path):
    csv_columns = ["NRF1", "RF1", "NRF2", "RF2", "NRF4", "RF4", "RF3", "RF5", "RF6"]
    data = [
        {
            "NRF1": "a1",
            "RF1": "b1",
            "NRF2": "c1",
            "RF2": "d1",
            "NRF4": "e1",
            "RF4": "f1",
            "RF3": "g1",
            "RF5": "h1",
            "RF6": "i1",
        },
        {
            "NRF1": "a2",
            "RF1": "b2",
            "NRF2": "c2",
            "RF2": "d2",
            "NRF4": "e2",
            "RF4": "f2",
            "RF3": "g2",
            "RF5": "h2",
            "RF6": "i2",
        },
        {
            "NRF1": "a3",
            "RF1": "b3",
            "NRF2": "c3",
            "RF2": "d3",
            "NRF4": "e3",
            "RF4": "f3",
            "RF3": "g3",
            "RF5": "h3",
            "RF6": "i3",
        },
        {
            "NRF1": "a3",
            "RF1": "b3",
            "NRF2": "c3",
            "RF2": "d3",
            "NRF4": "e3",
            "RF4": "f3",
            "RF3": "g3",
            "RF5": "h3",
            "RF6": "i3",
        },
        {
            "NRF1": "a4",
            "RF1": "b3",
            "NRF2": "c4",
            "RF2": "d3",
            "NRF4": "e4",
            "RF4": "f3",
            "RF3": "g3",
            "RF5": "h3",
            "RF6": "i3",
        },
    ]

    unique_ms_rows = 3

    nms_fields = ["NRF1", "NRF2", "NRF4"]
    csv_file = "input_file2.csv"
    d = tmp_path / "data1"
    d.mkdir()
    p = d / csv_file
    with open(str(p), "w") as csvfile:
        writer = DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return str(p), nms_fields, unique_ms_rows


@pytest.fixture
def wrong_file_path(tmp_path):
    p = tmp_path / "non_existent_input.csv"
    return str(p)


@pytest.fixture
def basic_input_file(tmp_path):
    csv_columns = ["FNAME", "LNAME", "ADD1", "ADD2", "CITY", "STATE", "ZIP", "SEQ"]
    dict_data = {
        "FNAME": "JOHN",
        "LNAME": "DOE",
        "ADD1": "1553 NEW GARDEN RD",
        "ADD2": "1F",
        "CITY": "GREENSBORO",
        "STATE": "NC",
        "ZIP": "27410",
        "SEQ": "123",
    }
    csv_file = "input2.csv"
    d = tmp_path / "data"
    d.mkdir()
    p = d / csv_file
    with open(str(p), "w") as csvfile:
        writer = DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerow(dict_data)
    return str(p)


@pytest.fixture
def dataframe():
    data = [["a1", "b1", "c1", "d1", "e1"], ["a2", "b2", "c2", "d2", "e2"]]
    dataframe = pd.DataFrame(data)
    dataframe.columns = ["Column1", "Column2", 1, "Column3", "Column4"]
    return dataframe
