import os
import glob
import subprocess
import pandas as pd
from pymongo import MongoClient
from wheel_inspect import inspect_wheel

pypi_db = MongoClient(host="127.0.0.1", port=27017)['pypi']
dl_packages = pypi_db['dl_packages']
distribution_metadata = pypi_db['distribution_metadata']

WHEEL_DIR = "/data/pkg_wheels"

def sort_versions(package: str) -> list:
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


def get_latest_versions(package: str) -> str:
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
    sorted_versions = sort_versions(package)
    res.sort(key=sorted_versions.get)
    return res[-1]


def download_wheels(package: str, version: str):
    command = ["pip", "download", "--no-deps", "-i", "https://pypi.tuna.tsinghua.edu.cn/simple",
               "-d", WHEEL_DIR, "--pre", f"{package}=={version}"]
    try:
        subprocess.check_call(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print(f"Downloading {package} {version} error")
        return 0
    filenames = glob.glob(f"{WHEEL_DIR}/{package}-{version}*.whl")
    if filenames:
        return filenames[0]
    else:
        return 0


def get_import_names(package: str, version: str) -> list:
    whl_name = download_wheels(package, version)
    if whl_name == 0:
        return []
    else:
        whl_path = os.path.join(WHEEL_DIR, whl_name)
        whl_metadata = inspect_wheel(whl_path)
        if "top_level" in whl_metadata["dist_info"]:
            return whl_metadata["dist_info"]["top_level"]

if __name__ == "__main__":
    # print(sort_versions("mlmodels"))
    # print(get_latest_versions("mlmodels"))
    print(get_import_names("matplotlib", "3.5.1"))
