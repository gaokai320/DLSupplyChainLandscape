import pandas as pd
from pymongo import MongoClient

LAYER1 = ['tensorflow', 'tensorflow-cpu', 'tensorflow-gpu', 'torch', 'mxnet', 'mxnet-cu112', 'mxnet-cu110', 'mxnet-cu102', 'mxnet-cu102mkl', 'mxnet-cu101', 'mxnet-cu101mkl', 'mxnet-cu100', 'mxnet-cu100mkl',
          'mxnet-cu92', 'mxnet-cu92mkl', 'mxnet-cu90', 'mxnet-cu90mkl', 'mxnet-cu80', 'mxnet-cu80mkl', 'mxnet-native', 'paddlepaddle', 'paddlepaddle-gpu', 'mindspore', 'mindspore-ascend', 'mindspore-gpu']

client = MongoClient(host='127.0.0.1', port=27017)

filter = {
    'layer': {
        '$gt': 1
    }
}
project = {
    '_id': 0,
    'name': 1,
    'dependency': 1,
    'framework': 1
}

result = client['pypi']['dl_packages'].find(
    filter=filter,
    projection=project
)

df = pd.DataFrame(list(result)).drop_duplicates()
df = df[~(df['name'].isin(LAYER1))]
print(df.groupby('framework')['name'].count())

df.to_csv("data/dag.edgs", index=False, header=True)
