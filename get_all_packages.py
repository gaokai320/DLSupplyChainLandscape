import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

db = MongoClient(host='127.0.0.1', port=27017)['pypi']
distribution_metadata = db['distribution_metadata']
dependencies = db['dependencies']

# exclude_packages = ['mkdocs', 'nbdev', 'ghapi',
#                     'jupyter-releaser', 'voila', 'gensim', 'nltk', 'fastcore']


def get_latest_version(df):
    return df.sort_values(by=['version'], ascending=False).iloc[0]


def group(df):
    gpr = df.groupby(by=['name']).apply(get_latest_version)
    return


def get_official_packages(url: str):
    query = {
        "$or": [{"download_url": {"$regex": url, "$options": "$i"}},
                {"home_page": {"$regex": url, "$options": "$i"}},
                {"project_urls": {"$elemMatch": {"$regex": url, "$options": "$i"}}}]
    }
    query_results = list(distribution_metadata.find(query, projection={"_id": False, "name": True, "version": True,
                                                                       "summary": True, "home_page": True, "project_urls": True, "download_url": True}))
    if len(query_results) > 0:
        df = pd.DataFrame(query_results)
        res = pd.DataFrame()
        gpr = df.groupby(by=['name']).apply(get_latest_version)
        res['name'] = gpr.get('name')
        res['summary'] = gpr.get('summary')
        res['home_page'] = gpr.get('home_page')
        res['project_urls'] = gpr.get('project_urls')
        res['download_url'] = gpr.get('download_url')
        res['layer'] = 2
        unique_names = list(res['name'].unique())
        return unique_names, res
    else:
        return [], pd.DataFrame()


def get_downstream_packages(packages: list, prior_packages: list, layer: int, df: pd.DataFrame):
    for pkg in packages:
        query = {
            "dependency": pkg,
            "extra": False
        }
        query_results = list(dependencies.find(query, projection={
                             "_id": False, "name": True, "version": True, "dependency": True}))
        if len(query_results) > 0:
            tmp = pd.DataFrame(query_results)
            new_df = pd.DataFrame()
            for row in tmp.iterrows():
                name = row[1]['name']
                version = row[1]['version']
                new_query = {
                    "name": name,
                    "version": version
                }
                try:
                    new_results = list(distribution_metadata.find(new_query, projection={
                                       "_id": False, "name": True, "version": True, "summary": True, "home_page": True, "project_urls": True, "download_url": True}))
                    new_df = new_df.append(pd.DataFrame(
                        new_results), ignore_index=True)
                except:
                    print(name, version)
            gpr = new_df.groupby(by=['name']).apply(get_latest_version)
            new_df = pd.DataFrame()
            new_df['name'] = gpr.get('name')
            new_df['summary'] = gpr.get('summary')
            new_df['home_page'] = gpr.get('home_page')
            new_df['project_urls'] = gpr.get('project_urls')
            new_df['download_url'] = gpr.get('download_url')
            new_df['layer'] = layer
            new_df['dependency'] = pkg
            df.loc[df['name'] == pkg, 'dependent_number'] = len(
                new_df['name'].unique())
            df = df.append(new_df)
    # df.to_csv('tmp.csv', index=False)
    return list(set(list(df['name'])) - set(prior_packages)), df


def get_all_packages(package: list, url: str):
    # get all official packages share the same url
    layer = 1
    print("Find {} packages in layer {}".format(len(package), layer))
    all_packages = []
    official_packages, df = get_official_packages(url)
    df.loc[df['name'].isin(package), 'layer'] = 1
    print("{} official packages in layer 2".format(
        len(official_packages) - len(package)))
    all_packages.extend(official_packages)
    layer += 1
    downstream_packages, df = get_downstream_packages(
        package, all_packages, layer, df)
    print("Find another {} packages in layer 2".format(
        len(downstream_packages), layer))
    # get all downstream packages
    all_packages.extend(downstream_packages)
    downstream_packages = list(set(all_packages) - set(package))

    while(downstream_packages):
        layer += 1
        downstream_packages, df = get_downstream_packages(
            downstream_packages, all_packages, layer, df)
        all_packages.extend(downstream_packages)

        print("Find {} packages in layer {}".format(
            len(downstream_packages), layer))
    print(len(df), len(all_packages))
    df.to_csv('data/' + package[0] + '.csv', index=False)


if __name__ == '__main__':
    get_all_packages(['mindspore', 'mindspore-ascend',
                      'mindspore-gpu'], 'https://gitee.com/mindspore')
    print('mindspore done')
    get_all_packages(['paddlepaddle', 'paddlepaddle-gpu'],
                     'https://github.com/paddlepaddle')
    print('paddlepaddle done')
    get_all_packages(['torch'], 'https://github.com/pytorch')
    print('pytorch done')
    get_all_packages(['tensorflow', 'intel-tensorflow', 'intel-tensorflow-avx512', 'tensorflow-aarch64', 'tensorflow-ascend',
                      'tensorflow-cpu', 'tensorflow-fedora28', 'tensorflow-gpu', 'tensorflow-macos', 'tf-nightly', 'tf-nightly-cpu',
                      'tf-nightly-gpu', 'tf-nightly-xla-gpu'],
                     'https://github.com/tensorflow')
    print('tensorflow done')
