import os
import json
import wget
import logging
import requests
from typing import List, Optional
import multiprocessing as mp

WHEEL_DIR = "/data/kyle/pkg_wheels"


def _select_wheel(file_info: List[dict]) -> Optional[dict]:
    result = None
    result_py_ver = ""
    for i in file_info:
        if not i["filename"] or not i["filename"].endswith(".whl"):
            continue
        # Parse wheel file name according to https://www.python.org/dev/peps/pep-0427/
        tags = i["filename"].split("-")
        if len(tags) <= 4 or len(tags) >= 7:  # bad filename
            continue
        elif len(tags) == 5:
            py_ver, platform = tags[2], tags[4]
        else:
            py_ver, platform = tags[3], tags[5]
        if ("any" in platform or "linux" in platform) and py_ver > result_py_ver:
            result = i
            result_py_ver = py_ver
    return result


def download_wheel(package: str, version: str):
    try:
        pypi_info = requests.get(
            f"https://pypi.org/pypi/{package}/json").json()
    except json.decoder.JSONDecodeError:
        logging.error(f"{package} does not exist on PyPI")
    try:
        file_infos = pypi_info['releases'][version]
    except:
        logging.error(f"{package} {version} does not exist on PyPI")
    whl_file = _select_wheel(file_infos)
    if whl_file is not None:
        store_path = os.path.join(WHEEL_DIR, whl_file['filename'])
        if os.path.exists(store_path):
            logging.info(f"{whl_file['filename']} already exists")
            return whl_file['filename']
        else:
            wget.download(whl_file['url'], store_path)


if __name__ == "__main__":
    download_wheel("tensorflow-addons", "0.7.1")
