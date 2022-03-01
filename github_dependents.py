import os
import json
import time
import random
import logging
import requests
from collections import defaultdict
from bs4 import BeautifulSoup

TOKEN = json.load(open('gh_tokens.json'))['token']
# TOKEN = "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"
LOG_PATH = "log/downstream_repos.log"
DATA_PATH = "data/pkg_github_dependents.json"


def get_python_packge_url(pkg: str, url: str):
    headers = {"Authorization": f"token {TOKEN}"}
    request_url = f"https://{url}/network/dependents?dependent_type=REPOSITORY"
    try:
        response = requests.get(request_url, headers=headers)
    except requests.exceptions.RequestException as e:
        logging.error(f"{pkg}, {url}: {e}")
        return "", None
    if response.status_code == 404:
        logging.error(f"{pkg}, Not found: {url}")
        return "", None
    soup = BeautifulSoup(response.content, 'lxml')
    menus = soup.find("div", {"class": "select-menu-list"})
    if not menus:
        logging.info("Only one package")
        return request_url, response
    else:
        for i, t in enumerate(menus.findAll('a')):
            name = t.find('span', {"class": "select-menu-item-text"}).text
            name = name.strip('\n ')
            if name == pkg:
                if i == 0:
                    logging.info('Hit cache')
                    return request_url, response
                else:
                    request_url = f"https://github.com{t['href']}"
                    return request_url, None
    return "", None


def parse_html(response: requests.Response, headers: dict, pkg: str) -> list:
    dependents = []
    flag = response.ok
    page = 1
    while (flag):
        soup = BeautifulSoup(response.content, 'lxml')
        try:
            val = soup.find("p", {"class": "mb-4"}).find("strong").text
            if val != pkg:
                return []
        except:
            return []
        tmp = [
            "{}/{}".format(
                t.find('a', {"data-repository-hovercards-enabled": ""}).text,
                t.find('a', {"data-hovercard-type": "repository"}).text
            )
            for t in soup.findAll("div", {"class": "Box-row", "data-test-id": "dg-repo-pkg-dependent"})
        ]
        logging.info(f"Page {page}: {len(tmp)} repositories")
        dependents.extend(tmp)
        btns = soup.find(
            "div", {"class": "paginate-container"})
        if btns:
            btns = btns.findAll('a')
        request_url = ""
        if btns:
            for b in btns:
                if b.text == 'Next':
                    request_url = b['href']
                    break
        if request_url != "":
            page += 1
            response = requests.get(request_url, headers=headers)
            logging.info(f"Page {page}: Status Code {response.status_code}")
            flag = response.ok
            time.sleep(random.randint(4, 8))
        else:
            break
    return dependents


def get_repositories(request_url: str, response: requests.Response, headers: dict, pkg: str) -> list:
    logging.info(f"Repository: {request_url}")
    if response is None:
        response = requests.get(request_url, headers=headers)
    logging.info(f"Page {1}: Status Code {response.status_code}")
    return parse_html(response, headers, pkg)


def get_packages(request_url: str, headers: dict, pkg: str) -> list:
    if "package_id" in request_url:
        package_id = request_url.split("package_id=")[1]
        prefix = request_url.split('?')[0]
        request_url = f"{prefix}?dependent_type=PACKAGE&package_id={package_id}"
    else:
        request_url = request_url.replace("REPOSITORY", "PACKAGE")
    logging.info(f"Package: {request_url}")
    response = requests.get(request_url, headers=headers)
    logging.info(f"Page {1}: Status Code: {response.status_code}")
    return parse_html(response, headers, pkg)


def github_dependents(pkg: str, url: str):
    headers = {"Authorization": f"token {TOKEN}"}
    request_url, response = get_python_packge_url(pkg, url)
    if request_url == "":
        return list(), list()
    repos = get_repositories(request_url, response, headers, pkg)
    pkgs = get_packages(request_url, headers, pkg)
    res = list(set(repos).union(set(pkgs)))
    logging.info(
        f"{pkg}: {len(repos)} repositories, {len(pkgs)} packages, {len(res)} dependents")
    return repos, pkgs


def test():
    # No repository location
    print(github_dependents("ADAFMNoiseReducer", ""))
    print("End ADAFMNoiseReducer")
    # 404 repository
    print(github_dependents("dnntime", "github.com/Kevin-Chen0/deep-time-series.git"))
    print("End dnntime")
    # No select menus, no dependents
    print(github_dependents("linformer-pytorch",
          "github.com/tatp22/linformer-pytorch"))
    print("End linformer-pytorch")
    # No select menu, has dependents in one page
    print(github_dependents("geniverse", "github.com/thegeniverse/geniverse"))
    print("End geniverse")
    # No select menu, has dependents in more than one pages
    print(len(github_dependents("tensorly", "github.com/tensorly/tensorly")))
    print("End tensorly")
    # Have select menu, pypi package not the default
    print(len(github_dependents("pytorch-ignite", "github.com/pytorch/ignite")))
    # Have select menu, pypi package the default
    print(len(github_dependents("autogluon", "github.com/awslabs/autogluon")))


def check_log(pkg2repo: dict, logpath: str):
    all_pkgs = dict()
    for p, r in pkg2repo.items():
        all_pkgs[p] = r
    finished_pkgs = defaultdict(dict)
    if not os.path.exists(logpath):
        return all_pkgs, finished_pkgs
    with open(logpath) as f:
        for line in f:
            if "[INFO] Dependent Repositories of " in line:
                info = line.strip('\n').split('Dependent Repositories of ')[1]
                pkg, deps = info.split(': ')
                deps = deps.strip('][').split(', ') if deps != '[]' else []
                deps = [d.strip(r"'\"") for d in deps]
                finished_pkgs[pkg]['Repositories'] = deps
            elif "[INFO] Dependent Packages of " in line:
                info = line.strip('\n').split('Dependent Packages of ')[1]
                pkg, deps = info.split(': ')
                deps = deps.strip('][').split(', ') if deps != '[]' else []
                deps = [d.strip(r"'\"") for d in deps]
                finished_pkgs[pkg]['Packages'] = deps
    remain_pkgs = set(all_pkgs.keys()) - set(finished_pkgs.keys())
    print(
        f"All packages: {len(all_pkgs)}, Finished packages: {len(finished_pkgs)}, Remaining: {len(remain_pkgs)}")
    return {p: all_pkgs[p] for p in remain_pkgs}, finished_pkgs


if __name__ == "__main__":
    if os.path.exists(DATA_PATH):
        print(f"{DATA_PATH} already exists")
    else:
        pkg2repo = json.load(open("data/pkg_repo_url.json"))
        remain_pkgs, finished_pkgs = check_log(
            pkg2repo, LOG_PATH)
        print(len(remain_pkgs), len(finished_pkgs))
        if len(remain_pkgs) == 0:
            outf = open(DATA_PATH, 'w')
            json.dump(finished_pkgs, outf)
            outf.close()
        else:
            logging.basicConfig(
                filename=LOG_PATH,
                filemode='a',
                format="%(asctime)s [%(levelname)s] %(message)s",
                level=logging.INFO
            )
            for package, url in remain_pkgs.items():
                logging.info(f"Begin {package} {url}")
                repos, pkgs = github_dependents(package, url)
                finished_pkgs[package]['Repositories'] = repos
                finished_pkgs[package]['Packages'] = pkgs
                logging.info(f"Dependent Repositories of {package}: {repos}")
                logging.info(f"Dependent Packages of {package}: {pkgs}")
                logging.info(f"Finish {package} {url}")
            logging.info("Finished!")
            outf = open(DATA_PATH, 'w')
            json.dump(finished_pkgs, outf)
            outf.close()
