import os
import re
import json
import logging
import requests

PATTERN = r"\b(?:github\.com/[a-zA-Z0-9_.-]+|" \
          r"bitbucket\.org/[a-zA-Z0-9_.-]+|" \
          r"gitlab\.com/(?:[a-zA-Z0-9_.-]+)+|" \
          r"sourceforge\.net/projects" \
          r")/[a-zA-Z0-9_.-]+"
URL_PATTERN = re.compile(PATTERN)
LOG_PATH = "log/pkg_repo_url.log"


def repo_url(package: str):
    try:
        pypi_info = requests.get(
            f"https://pypi.org/pypi/{package}/json"
        ).json()['info']
    except json.decoder.JSONDecodeError:
        logging.error(f"{package} does not exist on PyPI")
        return ''
    home_page, project_urls = pypi_info.get(
        'home_page', ''), pypi_info.get("project_urls", {})
    m = URL_PATTERN.search(home_page)
    if m:
        return m.group(0)
    if project_urls:
        for pu in project_urls.values():
            m = URL_PATTERN.search(pu)
            if m:
                return m.group(0)
    return ''


def check_log(pkgs):
    pkg_url = {}
    finished_pkgs = {}
    if not os.path.exists(LOG_PATH):
        for p in pkgs:
            pkg_url[p] = ""
        return pkgs, finished_pkgs
    with open(LOG_PATH) as f:
        for line in f:
            if "[INFO]" in line:
                pkg, url = line.strip('\n').split('[INFO] ')[1].split(': ')
                finished_pkgs[pkg] = url
    return list(set(pkgs) - set(finished_pkgs.keys())), finished_pkgs


if __name__ == "__main__":
    pkgs = list(json.load(open('data/pkg_import_names.json')).keys())
    remain_pkgs, finished_pkgs = check_log(pkgs)
    if len(remain_pkgs) > 0:
        logging.basicConfig(
            filename=LOG_PATH,
            filemode='a',
            format="%(asctime)s [%(levelname)s] %(message)s",
            level=logging.INFO,
        )
        for p in remain_pkgs:
            url = repo_url(p)
            logging.info(f"{p}: {url}")
            finished_pkgs[p] = url
    with open("data/pkg_repo_url.json", 'w') as outf:
        json.dump(finished_pkgs, outf)
    
