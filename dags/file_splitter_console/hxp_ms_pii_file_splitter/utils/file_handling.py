from csv import reader


def get_all_fields(filepath):
    with open(filepath, "r", encoding="utf-8-sig") as csvfile:
        csvreader = reader(csvfile)
        return next(csvreader)
