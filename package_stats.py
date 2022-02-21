import pandas as pd
from pymongo import MongoClient

pypi_db = MongoClient(host='127.0.0.1', port=27017)['pypi']
dl_packages = pypi_db['dl_packages']
distribution_metadata = pypi_db['distribution_metadata']


def dependent_count(dependency: str, framework: str) -> int:
    pipeline = [
        {
            "$match": {
                "framework": framework,
                "dependency": dependency
            }
        }, {
            "$group": {
                "_id": None,
                "dependents": {
                    "$addToSet": "$name"
                }
            }
        }, {
            "$project": {
                "count": {
                    "$size": "$dependents"
                }
            }
        }
    ]
    res = pd.DataFrame(list(dl_packages.aggregate(pipeline)))
    if res.empty:
        return 0
    return res['count'][0]


def unversioned_sc(frameworks: list):
    res = pd.DataFrame()
    for framework in frameworks:
        pipeline1 = [
            {
                "$match": {
                    "framework": framework
                }
            }, {
                "$group": {
                    "_id": "$name",
                    "layers": {
                        "$addToSet": "$layer"
                    }
                }
            }, {
                "$unwind": "$layers"
            }, {
                "$project": {
                    "_id": 0,
                    "package": "$_id",
                    "layer": "$layers"
                }
            }
        ]
        pkg_layers = pd.DataFrame(list(dl_packages.aggregate(pipeline1)))
        pkg_layers["framework"] = framework
        for p in pkg_layers['package'].unique():
            pkg_layers.loc[pkg_layers.package == p,
                           'dependent_number'] = dependent_count(p, framework)
        pkg_layers['dependent_number'] = pkg_layers['dependent_number'].astype(
            int)
        res = res.append(pkg_layers, ignore_index=True)
    return res


if __name__ == "__main__":
    frameworks = ['tensorflow', 'pytorch',
                  'mxnet', 'paddlepaddle', 'mindspore']
    pkg_layers = unversioned_sc(frameworks)
    pkg_layers.to_csv("data/package_statistics.csv", index=False)
    print(pkg_layers.head())
