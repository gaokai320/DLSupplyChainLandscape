import os
import json
import wget
import time
import logging
import requests
import pandas as pd
from typing import List, Optional
from pymongo import MongoClient
from wheel_inspect import inspect_wheel
import multiprocessing as mp


WHEEL_DIR = "/data/kyle/pkg_wheels"


def sort_versions(package: str, distribution_metadata) -> list:
    def latest_time(df):
        return df.sort_values('upload_time', ascending=False).iloc[0]
    query = {
        "name": package
    }
    projection = {
        "_id": 0,
        "version": 1,
        "upload_time": 1
    }
    tmp = pd.DataFrame(
        list(distribution_metadata.find(query, projection=projection)))
    tmp = tmp.groupby('version')['upload_time'].max().sort_values()
    res = dict()
    for i, v in enumerate(tmp.index):
        res[v] = i
    return res


def get_latest_version(package: str, dl_packages, distribution_metadata) -> str:
    pipeline = [
        {
            "$match": {
                "name": package,
                # "framework": framework
            }
        }, {
            "$group": {
                "_id": "$name",
                "versions": {
                    "$addToSet": "$version"
                }
            }
        }
    ]
    res = list(dl_packages.aggregate(pipeline))[0]['versions']
    sorted_versions = sort_versions(package, distribution_metadata)
    res.sort(key=sorted_versions.get)
    return res[-1]


def pkg2v(p: str):
    pypi_db = MongoClient(host="127.0.0.1", port=27017)['pypi']
    dl_packages = pypi_db['dl_packages']
    distribution_metadata = pypi_db['distribution_metadata']
    print(get_latest_version(p, dl_packages, distribution_metadata))


def get_pkg2latestv():
    pypi_db = MongoClient(host="127.0.0.1", port=27017)['pypi']
    dl_packages = pypi_db['dl_packages']
    distribution_metadata = pypi_db['distribution_metadata']
    df = pd.read_csv("data/package_statistics.csv")
    df = df[['package', 'layer']].groupby("package")['layer'].min()
    df = df[df > 1]
    packages = list(df.index)
    pkg2v = []
    for p in packages:
        v = get_latest_version(p, dl_packages, distribution_metadata)
        pkg2v.append((p, v))
    with open('data/pkg2latestv.json', 'w') as outf:
        json.dump(pkg2v, outf)
    return pkg2v


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
        return 0
    try:
        file_infos = pypi_info['releases'][version]
    except:
        logging.error(f"{package} {version} does not exist on PyPI")
        return 0
    whl_file = _select_wheel(file_infos)
    if whl_file is not None:
        store_path = os.path.join(WHEEL_DIR, whl_file['filename'])
        if os.path.exists(store_path):
            logging.info(f"{whl_file['filename']} already exists")
        else:
            logging.info(f"Downloading {whl_file['filename']}")
            wget.download(whl_file['url'], store_path)
            logging.info(f"Downloaded {whl_file['filename']}")
            time.sleep(1)
        return whl_file['filename']
    else:
        return 0


def get_import_names(package: str, version: str) -> list:
    whl_name = download_wheel(package, version)
    if whl_name == 0:
        return []
    else:
        whl_path = os.path.join(WHEEL_DIR, whl_name)
        logging.info(f"inspect {whl_name}")
        whl_metadata = inspect_wheel(whl_path)
        if "top_level" in whl_metadata["dist_info"]:
            return whl_metadata["dist_info"]["top_level"]
        else:
            return []


def check_log(pkg2v, logpath: str):
    all_pkgs = dict()
    for p, v in pkg2v:
        all_pkgs[p] = v
    finished_pkgs = dict()
    if not os.path.exists(logpath):
        return all_pkgs, dict()
    with open(logpath) as f:
        for line in f:
            if "[INFO] Import names" in line:
                pkg, names = line.strip(']\n').split(': [')
                pkg = pkg.split(' ')[-1]
                names = names.split(', ') if names != '' else []
                names = [n.strip(r"'\"") for n in names]
                finished_pkgs[pkg] = names
    remain_pkgs = set(all_pkgs.keys()) - set(finished_pkgs.keys())
    print(
        f"All packages: {len(all_pkgs)}, Finished packages: {len(finished_pkgs)}, Remaining: {len(remain_pkgs)}")
    return {p: all_pkgs[p] for p in remain_pkgs}, finished_pkgs


if __name__ == "__main__":
    if os.path.exists("data/pkg_import_names.json"):
        print("data/pkg_import_names.json already exists")
    else:
        if not os.path.exists("data/pkg2latestv.json"):
            pkg2v = get_pkg2latestv()
        else:
            pkg2v = json.load(open("data/pkg2latestv.json"))
        remain_pkgs, finished_pkgs = check_log(
            pkg2v, "log/top_level_packages.log")
        print(len(remain_pkgs), len(finished_pkgs))
        # print(remain_pkgs, finished_pkgs)
        outf = open("data/pkg_import_names.json", 'w')
        if len(remain_pkgs) == 0:
            json.dump(finished_pkgs, outf)
        else:
            logging.basicConfig(
                filename="log/top_level_packages.log",
                filemode='a',
                format="%(asctime)s (Process %(process)d) [%(levelname)s] %(message)s",
                level=logging.INFO
            )

            for package, version in remain_pkgs.items():
                logging.info(f"Begin {package} {version}")
                names = get_import_names(package, version)
                finished_pkgs[package] = names
                logging.info(f"Import names of {package}: {names}")
                logging.info(f"Finish {package} {version}")
            logging.info("Finished!")
            json.dump(finished_pkgs, outf)
        outf.close()
