import json
import re
from tqdm import tqdm
import logging
from pymongo import MongoClient
from collections import defaultdict
from packaging.requirements import Requirement, InvalidRequirement
from packaging.markers import UndefinedEnvironmentName

db = MongoClient(host="127.0.0.1", port=27017)['pypi']
distribution_metadata = db['distribution_metadata']
# print(distribution_metadata.count_documents({}))


def parse_requirement(requirement_str: str):
    try:
        req = Requirement(requirement_str)
    except InvalidRequirement:
        logging.error("InvalidRequirement: {}".format(requirement_str))
        return
    except:
        logging.error("ParseError: {}".format(requirement_str))
        return
    name, extras, specifier, marker = req.name, req.extras, req.specifier, req.marker
    if len(extras) > 0:
        name = name + '[' + ','.join(extras) + ']'
    specifier = [str(s) for s in specifier]
    extra = False
    if marker is not None:
        try:
            marker.evaluate()
        except UndefinedEnvironmentName:
            extra = True
        except:
            logging.error("MarkerError: {}".format(marker))
            extra = False
    return name, tuple(specifier), extra


def transform():
    dependencies = db['dependencies']
    dependencies.drop()
    dependencies = db['dependencies']
    deps = defaultdict(list)
    for doc in tqdm(distribution_metadata.find({}, {"name": 1, "version": 1, "_id": 0, "requires_dist": 1})):
        name = doc.get("name", "")
        version = doc.get('version', '')
        requires_dist = doc.get('requires_dist', [])
        key_name = name + ' ' + version
        for req in requires_dist:
            res = parse_requirement(req)
            if res is not None:
                dependency, dependency_version, extra = res
                deps[key_name].append((dependency, dependency_version, extra))
    print('Dumping to json file')
    with open('/fast/pypi/dependency_dump.json', 'w') as outf:
        json.dump(deps, outf)
    transformed_docs = []
    print('Inserting')
    for k, v in tqdm(deps.items()):
        name, version = k.split(' ', 1)
        for dependency, dependency_version, extra in set(v):
            transformed_docs.append({"name": name, "version": version,
                                     "dependency": dependency, "dependency_version": dependency_version,
                                     "extra": extra})
    print(len(transformed_docs))
    dependencies.insert_many(transformed_docs)
    print('Finished')


if __name__ == "__main__":
    # req = 'psycopg2[abc, def] (>=2.8) ; (platform_python_implementation != "PyPy") and extra=="postgres"'
    # print(parse_req(req))
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO,
                        filename='extract_dependency.log', filemode='w')
    transform()
    # test = [
    #     'psycopg2[abc, def] (>=2.8) ; (platform_python_implementation != "PyPy") and extra=="postgres"',
    #     "numpy",
    #     "sphinx-rtd-theme (==0.5.0) ; extra == 'all'",
    #     "sphinx-rtd-theme (=0.5.0) ; extra == 'all'",
    #     "django-configurations[database,email] ; extr == 'configuration'",
    #     "prompt-toolkit (<=2.0.10,>=2.0.4)",
    #     "importlib-resources ; python_version < '3.7'"
    # ]
    # for t in test:
    #     res = parse_requirement(t)
    #     if res is not None:
    #         name, specs, extra = res
    #         print(name, specs, extra)
