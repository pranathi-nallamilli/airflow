import pytest

from hxp_ms_pii_file_splitter.utils.file_handling import get_all_fields


@pytest.mark.usefixtures("basic_input_file")
def test_read_csv_basic(basic_input_file):
    fields = get_all_fields(basic_input_file)
    assert fields is not None


def test_filepath_none():
    with pytest.raises(TypeError):
        get_all_fields(None)


@pytest.mark.usefixtures("wrong_file_path")
def test_file_not_found(wrong_file_path):
    with pytest.raises(FileNotFoundError):
        get_all_fields(wrong_file_path)
