import copy
import json
from tqdm import tqdm
import requirements
from pymongo import MongoClient
from collections import defaultdict

db = MongoClient(host="127.0.0.1", port=27017)['pypi']
distribution_metadata = db['distribution_metadata']
print(distribution_metadata.count_documents({}))

def transform():
    dependencies = db['dependencies']
    dependencies.drop()
    dependencies = db['dependencies']
    cnt = 0
    deps = defaultdict(list)
    for doc in tqdm(distribution_metadata.find({}, {"name": 1, "version": 1, "_id": 0, "requires_dist": 1})):
        name = doc.get("name", "")
        version= doc.get('version', '')
        requires_dist = doc.get('requires_dist', [])
        key_name = name + ' ' + version
        for req in requires_dist:
            try:
                require = list(requirements.parse(req))[0]
                dependency, dependency_version = require.name, require.specs
                deps[key_name].append((dependency, dependency_version))
            except:
                pass
    print('Dumping to json file')
    with open('data.json', 'w') as outf:
        json.dump(deps, outf)
    transformed_docs = []
    print('Inserting')
    for k, v in tqdm(deps.items()):
        tuple_v = list()
        for d, dv in v:
            tdv = tuple([tuple(_) for _ in dv])
            tuple_v.append((d, tdv))
        name, version = k.split(' ', 1)
        for dependency, dependency_version in set(tuple_v):
            transformed_docs.append({"name": name, "version": version, "dependency": dependency, "dependency_version": dependency_version})
    print(len(transformed_docs))
    dependencies.insert_many(transformed_docs)
    print('Finished')

if __name__ == "__main__":
    transform()

