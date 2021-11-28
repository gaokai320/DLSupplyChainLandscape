import os
import pandas as pd
from pymongo import MongoClient

db = MongoClient(host='127.0.0.1', port=27017)['pypi']
distribution_metadata = db['distribution_metadata']
dependencies = db['dependencies']

def get_official_packages(url: str):
    query = {
        "$or":[{ "download_url": { "$regex": url, "$options": "$i" } }, 
               { "home_page": { "$regex": url, "$options": "$i" } }, 
               { "project_urls": { "$elemMatch": { "$regex": url, "$options": "$i" } } }]
    }
    query_results = list(distribution_metadata.find(query, projection={"_id": False, "name": True, "version": True,
                                                                       "summary": True, "home_page": True}))
    if len(query_results) > 0:
        df = pd.DataFrame(list(query_results))
        unique_names = list(df['name'].unique())
        return unique_names, df
    else:
        return [], pd.DataFrame()

def get_downstream_packages(packages: list, prior_packages: list, layer: int, package: str):
    log_file = open(package, 'a')
    df = pd.DataFrame()
    for pkg in packages:
        query = {
            "dependency": pkg
        }
        query_results = list(dependencies.find(query, projection={"_id": False, "name": True, "version": True}))
        if len(query_results) > 0:
            tmp = pd.DataFrame(query_results)
            log_file.write('{},{},{}\n'.format(pkg, layer, len(tmp['name'].unique())))
            df = df.append(tmp)
    log_file.close()
    if not df.empty:
        df.drop_duplicates(inplace=True)
        unique_names = list(set(list(df['name'])) - set(prior_packages))
        more_df = pd.DataFrame()
        new_query = {
            "$or": []
        }
        new_df = pd.DataFrame()
        for row in df.iterrows():
            name = row[1]['name']
            version = row[1].version
            new_query = {
                "name": name,
                "version": version
            }
            try:
                new_results = list(distribution_metadata.find(new_query, projection={"_id": False, "name": True, "version": True,
                                                                           "summary": True, "home_page": True}))
                new_df = new_df.append(pd.DataFrame(new_results), ignore_index=True)
            except:
                print(name, version)
        return unique_names, new_df
    else:
        return [], pd.DataFrame()

def get_latest_version(df):
    res = df[pd.notna(df.home_page)]
    if res.empty:
        return df.iloc[0]
    else:
        return res.sort_values(by=['version'], ascending=False).iloc[0]

def get_all_packages(package: str, url: str):
    # get all official packages share the same url
    log_file = open(package, 'w')
    log_file.close()
    layer = 1
    all_packages = []
    official_packages, df = get_official_packages(url)
    print("Find {} packages in layer {}".format(len(official_packages), layer))
    print(official_packages)
    all_packages.extend(official_packages)
    # get all downstream packages
    downstream_packages, next_df = get_downstream_packages(official_packages, all_packages, layer, package)
    all_packages.extend(downstream_packages)
    df = df.append(next_df)
    layer += 1
    print("Find {} packages in layer {}".format(len(downstream_packages), layer))
    while(downstream_packages):
        downstream_packages, next_df = get_downstream_packages(downstream_packages, all_packages, layer, package)
        all_packages.extend(downstream_packages)
        df = df.append(next_df)
        layer += 1
        print("Find {} packages in layer {}".format(len(downstream_packages), layer))
    # print(all_packages)
    
    with pd.ExcelWriter(os.path.join('.', 'data', package+'.xlsx'), engine='xlsxwriter', options={'strings_to_urls': False}) as writer:
        group_df = df.groupby(by=['name']).apply(get_latest_version)[['name', 'summary', 'home_page']]
        df['official'] = 0
        group_df['official'] = 0
        df['official'] = 0
        for name in official_packages:
            df.loc[df.name == name, 'official'] = 1
            group_df.loc[group_df.name == name, 'official'] = 1
        df.to_excel(writer, sheet_name=package, index=False)
        group_df.to_excel(writer, sheet_name=package+'_simple', index=False)
    return df

if __name__ == '__main__':
    # get_all_packages('mindspore', 'https://gitee.com/mindspore')
    print('mindspore done')
    # get_all_packages('paddlepaddle', 'https://github.com/paddlepaddle')
    print('paddlepaddle done')
    get_all_packages('torch', 'https://github.com/pytorch')
    print('pytorch done')
    # get_all_packages('tensorflow', 'https://github.com/tensorflow')
    print('tensorflow done')
    
