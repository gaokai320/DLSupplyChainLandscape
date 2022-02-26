# DLSupplyChainLandscape
The Landscape of Deep Learning Supply Chain

## Prepare versioned PyPI dependencies   
1. Get all the metadata for every distribution released on PyPI from [Google Big query](https://console.cloud.google.com/marketplace/product/gcp-public-data-pypi/pypi).
    ```SQL
    SELECT
      metadata_version, name, version, summary, author, author_email, maintainer, maintainer_email,
      license, keywords, classifiers, platform, home_page, download_url, requires_python, requires,
      provides, obsoletes, requires_dist, provides_dist, obsoletes_dist, requires_external, project_urls,
      upload_time, filename, size, python_version, packagetype, comment_text
    FROM
      `bigquery-public-data.pypi.distribution_metadata`
    ```

2. Download query results to local file `/fast/pypi/distribution_metadata.json` and import to MongoDB database.
    ```shell
    mongoimport --db=pypi --collection=distribution_metadata --quiet --drop --numInsertionWorkers=8 --file=/fast/pypi/distribution_metadata.json
    ```

3. Parse each package's dependencies from `requires_dist` field.
    ```shell
    python extract_dependencies.py
    ```

4. Parse dependencies versions
    ```shell
    python versioned_packages.py
    ```
## Build DL SC
1. Collect package metadata from DL SC
    ```shell
    python dl_package_metadata.py
    ```
2. Get the number of dependent packages for each package
    ```shell
    python package_stats.py
    ```
3. Get GitHub dependents for each package
    ```shell
    # Get the repository url
    python pkg_repo_url.py
    # Crawl and parse the dependency network page
    python github_dependents.py
    ```
4. Get WoC dependents for each package
    ```shell
    # Get import names for each package
    python top_level_packages.py
    # extract python dependencies from woc
    python build_woc_dbs.py
    ```