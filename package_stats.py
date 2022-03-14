import json
import os
import pandas as pd
from pymongo import MongoClient

pypi_db = MongoClient(host='127.0.0.1', port=27017)['pypi']
dl_packages = pypi_db['dl_packages']
distribution_metadata = pypi_db['distribution_metadata']
import_names = json.load(open("data/curated_pkg_import_names.json"))

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
                           'down_pkgs'] = dependent_count(p, framework)
        pkg_layers['down_pkgs'] = pkg_layers['down_pkgs'].astype(
            int)
        res = res.append(pkg_layers, ignore_index=True)
    return res


def append_deps(pkg_layers):
    if os.path.exists("data/pkg_github_dependents.json") and os.path.exists("data/pkg_woc_dependents.json"):
        woc_dependents = json.load(open("data/pkg_woc_dependents.json"))
        gh_dependents = json.load(open("data/pkg_github_dependents.json"))
        gh_dependents = {k: {
            "Repositories": [_.replace('/', '_') for _ in v['Repositories']],
            "Packages": [_.replace('/', '_') for _ in v['Packages']]
        } for k, v in gh_dependents.items()}

        def map_gh_repo(x):
            if x in gh_dependents.keys():
                return len(gh_dependents[x]["Repositories"])
            return 0

        def map_gh_repo2(x):
            if x in gh_dependents.keys():
                return len(set(gh_dependents[x]["Repositories"]) - set(gh_dependents[x]["Packages"]))
            return 0
        
        def map_gh_repo3(x):
            if x in gh_dependents.keys():
                return len(set(gh_dependents[x]["Repositories"]).union(set(gh_dependents[x]["Packages"])))
            return 0

        def map_woc_repo(x):
            if x in woc_dependents.keys():
                return len(woc_dependents[x])
            return 0

        def map_woc_repo2(x):
            if x in woc_dependents.keys():
                return len(set(woc_dependents[x]) - set(gh_dependents[x]["Packages"]))
            return 0

        def map_all_repo(x):
            if x in woc_dependents.keys():
                return len(set(gh_dependents[x]["Repositories"]).union(set(woc_dependents[x])))
            return 0

        def map_all_repo2(x):
            if x in woc_dependents.keys():
                return len(set(gh_dependents[x]["Repositories"]).union(set(woc_dependents[x])) - set(gh_dependents[x]["Packages"]))
            return 0
        
        def map_all_repo3(x):
            if x in woc_dependents.keys():
                return len(set(gh_dependents[x]["Repositories"]).union(set(woc_dependents[x])).union(set(gh_dependents[x]["Packages"])))
            return 0

        # pkg_layers['gh_down_repos'] = pkg_layers['package'].map(map_gh_repo)
        # pkg_layers['gh_down_repos2'] = pkg_layers['package'].map(map_gh_repo2)
        # pkg_layers['woc_down_repos'] = pkg_layers['package'].map(map_woc_repo)
        # pkg_layers['woc_down_repos2'] = pkg_layers['package'].map(
        #     map_woc_repo2)
        # pkg_layers['comb_down_repos'] = pkg_layers['package'].map(map_all_repo)
        # pkg_layers['comb_down_repos2'] = pkg_layers['package'].map(
        #     map_all_repo2)
        pkg_layers['gh_downstream'] = pkg_layers['package'].map(map_gh_repo3)
        pkg_layers['woc_downstream'] = pkg_layers['package'].map(map_woc_repo)
        pkg_layers['comb_downstream'] = pkg_layers['package'].map(map_all_repo3)
        pkg_layers = pkg_layers.fillna(0)
    return pkg_layers


if __name__ == "__main__":
    frameworks = ['tensorflow', 'pytorch',
                  'mxnet', 'paddlepaddle', 'mindspore']
    pkg_layers = unversioned_sc(frameworks)
    pkg_layers = pkg_layers[~(pkg_layers['package'].isin(['tensorflow', 'tensorflow-gpu']))]
    pkg_layers['import_name'] = pkg_layers['package'].map(import_names)
    pkg_layers = append_deps(pkg_layers)
    layer1 = pkg_layers[pkg_layers.layer == 1]['package']
    pkg_layers = pkg_layers[~(pkg_layers.package.isin(layer1))]
    pkg_layers = pkg_layers[['package', 'import_name', 'layer', 'framework', 'down_pkgs', 'gh_downstream', 'woc_downstream', 'comb_downstream']]
    pkg_layers.to_csv("data/package_statistics.csv", index=False)
    print(pkg_layers)
