import json
import pandas as pd
import numpy as np
from Levenshtein import distance as levenshtein_distance


pkg_urls = json.load(open("data/pkg_repo_url.json"))
gh_dependents = json.load(open("data/pkg_github_dependents.json"))
gh_dependents = {k: (len(set(v['Repositories'])), len(
    set(v['Packages']))) for k, v in gh_dependents.items()}

pkg_names = json.load(open("data/pkg_import_names.json"))
dedup_import_names = json.load(open("data/dedup_pkg_names.json"))
exclusion = json.load(open("data/exclude_import_names.json"))['_']

woc = json.load(open("/data/technical_dependency/python_dependencies"))

DUP_NAMES = ['tests', 'test', 'examples', 'utils', 'scripts', 'src', 'models', 'docs', 'data', 'model', '_foo', 'tools', 'dev', 'datasets', 'main', 'samples', 'dataset', 'tutorials', 'app', 'configs', 'preprocessing', 'example', 'core', 'experiments', 'about', 'home', 'helpers', 'notebooks', 'metrics',
             'ml', 'build', 'exclude', 'Examples', 'Tutorials', 'bin', 'api', 'CONSTANTS', 'block', 'legacy', 'external', 'training', 'conf', '__init__', 'util', 'cmake_example', 'config', 'cli', '*', 'contributed', 'nbdev_template', 'forks', 'train', 'official', 'deployment', 'nets', 'tensorflow_examples']


def get_same_url_pkgs():
    url_pkgs = dict()
    for k, v in pkg_urls.items():
        if v != "":
            url_pkgs[v] = url_pkgs.get(v, [])
            url_pkgs[v].append(k)
    res = []
    for k, v in url_pkgs.items():
        if len(v) > 1:
            for _ in v:
                res.append({"url": k, "package": _, "gh_repos": gh_dependents[_][0],
                            "gh_pkgs": gh_dependents[_][1]})
    return pd.DataFrame(res)


def get_same_name_pkgs():
    name_pkgs = dict()
    for k, v in pkg_names.items():
        for n in v:
            name_pkgs[n] = name_pkgs.get(n, [])
            name_pkgs[n].append(k)
    res = dict()
    for k, v in name_pkgs.items():
        if len(v) > 1:
            res[k] = v
    res = sorted(res.items(), key=lambda x: len(x[1]), reverse=True)
    return res


def remove_names(names: list):
    for k, v in pkg_names.items():
        for n in names:
            if n in v:
                pkg_names[k].remove(n)


def filter_names():
    res = dict()
    deps = dict()
    for k, v in dedup_import_names.items():
        v = list(set(v) - set(exclusion))
        if v == []:
            res[k] = ""
            deps[k] = []
        else:
            d = [levenshtein_distance(k, _) for _ in v]
            tmp = v[np.argmin(d)]
            res[k] = tmp
            deps[k] = woc.get(tmp, [])
    return res, deps


if __name__ == "__main__":
    res, deps = filter_names()
    with open('data/curated_pkg_import_names.json', 'w') as outf:
        json.dump(res, outf)
    with open('data/pkg_woc_dependents.json', 'w') as outf:
        json.dump(deps, outf)
