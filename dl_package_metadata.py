from tabnanny import check
import numpy as np
import pandas as pd
import logging
from tqdm import tqdm
from pymongo import MongoClient
from packaging.version import Version

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.FileHandler('log/dl_package_metadata.log', 'w', 'utf-8')
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)

pypi_db = MongoClient(host='127.0.0.1', port=27017)['pypi']
distribution_metadata = pypi_db['distribution_metadata']
versioned_dependencies = pypi_db['versioned_dependencies']


def all_versions(pkg: str) -> list:
    pipeline = [
        {"$match": {"name": pkg}},
        {"$group": {"_id": None, "versions": {"$addToSet": "$version"}}}
    ]
    versions = list(distribution_metadata.aggregate(pipeline=pipeline))
    if versions:
        return sorted(versions[0]['versions'], key=Version)
    else:
        return []


def direct_dependents(package: str, versions: list):
    query = {
        "dependency": package,
        "dependency_version": {
            "$in": versions
        },
        "extra": False
    }
    query_results = versioned_dependencies.find(query, projection={
                                                "_id": 0, "name": 1, "version": 1, "dependency": 1, "dependency_version": 1})
    return pd.DataFrame(list(query_results))


def next_layer(packages: list, versions: list):
    res = pd.DataFrame()
    assert len(packages) == len(versions)
    for p, v in zip(packages, versions):
        tmp = direct_dependents(p, v)
        if not tmp.empty:
            res = res.append(direct_dependents(p, v))
    return res


def all_layers(packages: list):
    layer = 1
    res = pd.DataFrame()
    versions = []
    for p in packages:
        versions.append(all_versions(p))
        res = res.append(pd.DataFrame(
            {"name": p, "version": all_versions(p)}))
    res.loc[:, 'layer'] = layer
    # print(res)
    while packages:
        layer = layer + 1
        logging.info(
            f'===============Finding layer {layer} packages===============')
        nl = next_layer(packages, versions)
        logging.info(f"Finding {len(nl)} releases.")
        if nl.empty:
            break
        nl.loc[:, 'layer'] = layer
        next_pkgs = nl[['name', 'version']].drop_duplicates()
        prior_pkgs = res[['name', 'version']].drop_duplicates()
        new_pkgs = next_pkgs[~(next_pkgs['name'].isin(prior_pkgs['name']) &
                               next_pkgs['version'].isin(prior_pkgs['version']))]
        logging.info(
            f"{len(next_pkgs)} unique releases, {len(new_pkgs)} new releases")
        new_pkgs = new_pkgs.groupby('name')['version'].apply(list)
        logging.info(f"{len(new_pkgs)} new packages")
        res = res.append(nl)
        packages = list(new_pkgs.index)
        versions = list(new_pkgs.values)
    return res


def insert_db():
    coll = pypi_db['dl_packages']
    coll.drop()
    coll = pypi_db['dl_packages']
    res = all_layers(['tensorflow', 'tensorflow-cpu', 'tensorflow-gpu'])
    res.loc[:, 'framework'] = 'tensorflow'
    coll.insert_many(res.to_dict("records"))
    res = all_layers(['torch'])
    res.loc[:, 'framework'] = 'pytorch'
    coll.insert_many(res.to_dict("records"))
    res = all_layers(['mxnet', 'mxnet-cu112', 'mxnet-cu110', 'mxnet-cu102', 'mxnet-cu102mkl', 'mxnet-cu101', 'mxnet-cu101mkl', 'mxnet-cu100',
                     'mxnet-cu100mkl', 'mxnet-cu92', 'mxnet-cu92mkl', 'mxnet-cu90', 'mxnet-cu90mkl', 'mxnet-cu80', 'mxnet-cu80mkl', 'mxnet-native'])
    res.loc[:, 'framework'] = 'mxnet'
    coll.insert_many(res.to_dict("records"))
    res = all_layers(['paddlepaddle', 'paddlepaddle-gpu'])
    res.loc[:, 'framework'] = 'paddlepaddle'
    coll.insert_many(res.to_dict("records"))
    res = all_layers(['mindspore', 'mindspore-ascend', 'mindspore-gpu'])
    res.loc[:, 'framework'] = 'mindspore'
    coll.insert_many(res.to_dict("records"))


def check_version(v):
    try:
        v = Version(v)
        return v.is_prerelease or v.is_devrelease
    except:
        return True


def remove_prereleases():
    coll = pypi_db['dl_packages']
    print(f"Original: {coll.count_documents({})} documents")
    cnt = 0
    for doc in tqdm(coll.find({})):
        version, dependency_version = doc['version'], doc['dependency_version']
        if check_version(version) or check_version(dependency_version):
            cnt += 1
            coll.delete_one(doc)
    print(f"Delete: {cnt} documents")
    print(f"Final: {coll.count_documents({})} documents")


if __name__ == "__main__":
    insert_db()
    remove_prereleases()
