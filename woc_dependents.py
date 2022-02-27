import json
from isort import file
import numpy as np
from Levenshtein import distance as levenshtein_distance

WOC_PATH = "/data/technical_dependency/python_dependencies"
RAW_IN_PATH = "data/pkg_import_names.json"
EXCLUSION_PATH = "data/exclude_import_names.json"
CURATED_IN_PATH = "data/curated_pkg_import_names.json"
OUT_PATH = "data/pkg_woc_dependents.json"

woc_dependents = json.load(open(WOC_PATH))
pkg_ins = json.load(open(RAW_IN_PATH))
exclusions = json.load(open(EXCLUSION_PATH))


def filter_names():
    res = dict()
    for k, v in pkg_ins.items():
        exclude = exclusions.get(k, exclusions['_'])
        v = list(set(v) - set(exclude) - set(exclusions['_']))
        if v == []:
            res[k] = ''
        else:
            d = [levenshtein_distance(k, v) for v in v]
            # 5 is set based on experiments.
            if np.min(d) <= 5:
                res[k] = v[np.argmin(d)]
            else:
                res[k] = ''
    with open(CURATED_IN_PATH, 'w') as outf:
        json.dump(res, outf)
    return res


def get_woc_dependents():
    res = dict()
    curated_pkg_ins = filter_names()
    for pkg, names in curated_pkg_ins.items():
        if names == "":
            res[pkg] = []
        else:
            res[pkg] = woc_dependents.get(names, [])
    with open(OUT_PATH, 'w') as outf:
        json.dump(res, outf)


if __name__ == "__main__":
    get_woc_dependents()
