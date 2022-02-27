from distutils.log import INFO
import json
import os
import logging
import gzip
import joblib
from joblib import Parallel, delayed

DEPENDENCY_MAP_PATH = "/data/technical_dependency/c2PtabllfPkgFullT{}.s"
OUTPUT_DIR = "/data/technical_dependency"


def extract_python_dependencies(index: int):
    if os.path.exists(f'{os.path.join(OUTPUT_DIR, "python_dependencies")}.{index}'):
        logging.info(
            f'{os.path.join(OUTPUT_DIR, "python_dependencies")}.{index} already exists')
        return
    res = dict()
    lineno = 0
    with gzip.open(DEPENDENCY_MAP_PATH.format(index), 'rt', errors='ignore') as f:
        for line in f:
            lineno += 1
            try:
                items = line.strip('\n').split(';')
                repo, lang, modules = items[1], items[6], items[8:]
                if lang == 'Python':
                    for m in modules:
                        pkg = m.split('.')[0]
                        res[pkg] = res.get(pkg, [])
                        res[pkg].append(repo)
            except Exception as e:
                print(index, lineno, line, e)
    for k, v in res.items():
        res[k] = list(set(v))
    with open(f'{os.path.join(OUTPUT_DIR, "python_dependencies")}.{index}', 'w') as outf:
        json.dump(res, outf)


def combine_dependencies():
    if os.path.exists(os.path.join(OUTPUT_DIR, "python_dependencies")):
        logging.info(f'{os.path.join(OUTPUT_DIR, "python_dependencies")} already exists')
        return
    res = json.load(
        open(f'{os.path.join(OUTPUT_DIR, "python_dependencies")}.0'))
    for i in range(1, 128):
        tmp = json.load(
            open(f'{os.path.join(OUTPUT_DIR, "python_dependencies")}.{i}'))
        for k in tmp.keys():
            res[k] = res.get(k, [])
            res[k].extend(tmp[k])
    for k, v in res.items():
        res[k] = list(set(v))
    with open(os.path.join(OUTPUT_DIR, "python_dependencies"), 'w') as outf:
        json.dump(res, outf)


def run():
    Parallel(n_jobs=joblib.cpu_count())(
        delayed(extract_python_dependencies)(i) for i in range(128))
    combine_dependencies()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO
    )
    combine_dependencies()
