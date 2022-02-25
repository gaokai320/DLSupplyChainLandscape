import gunzip
from joblib import Parallel

DEPENDENCY_MAP_PATH = "/data/technical_dependency/c2PtabllfPkgFullT{}.s"

def extract_python_dependencies(index: int):
    with gunzip.open(DEPENDENCY_MAP_PATH.format(index), 'rb') as f:
        for line in f:
            items = line.strip('\n').split(';')
            repo, t, lang, modules = items[1], items[2], items[6], items[8:]
            