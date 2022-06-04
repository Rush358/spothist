import json
from datetime import datetime
from pathlib import Path

from prefect import task

from definitions import ROOT_DIR


@task
def write_dict_to_json(dictionary: dict, directory: str, filename: str):
    """
    Takes in (Python) dict and writes it as a JSON file in a specified directory.
    """

    str_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')
    filename_timestamp = f"{filename}_{str_timestamp}.json"

    file_path = ROOT_DIR / directory / filename_timestamp

    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file, indent=4)


@task
def build_archive_directory() -> Path:
    """
    Builds a directory to archive the current run based on run date and time, and returns its path.
    """

    str_date = datetime.now().strftime("%Y-%m-%d")
    dir_archive = ROOT_DIR / 'data' / str_date

    Path(dir_archive).mkdir(parents=False, exist_ok=True)

    return dir_archive
