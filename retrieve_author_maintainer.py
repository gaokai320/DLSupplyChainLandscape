import os
import json
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://pypi.org/project/{}/"
DATA_PATH = "data/package_author_maintainers.json"
LOG_PATH = "log/retrieve_author_maintainer.log"


def download_html(name: str):
    try:
        response = requests.get(BASE_URL.format(name))
    except requests.exceptions.RequestException as e:
        logging.error(f"{name}: {e}")
    if response.status_code == 404:
        return None
    return BeautifulSoup(response.content, "lxml")


def parse_html(soup: BeautifulSoup, name: str):
    res = dict()
    res["author"] = dict()
    res["maintainers"] = []
    if soup is None:
        return res
    try:
        sidebars = soup.find_all("div", {"class": "sidebar-section"})
        has_meta, has_maintainers = False, False
        for sb in sidebars:
            title = sb.find("h3", {"class": "sidebar-section__title"})
            if not title:
                continue
            if (not has_meta) and (title.text == "Meta"):
                for p in sb.find_all(lambda tag: tag.name == 'p' and not tag.attrs):
                    if p.find("strong").text == "Author:":
                        try:
                            author_name = p.find("a").text
                            author_email = p.find("a")["href"][7:]
                            res["author"]["name"] = author_name
                            res["author"]["email"] = author_email
                        except:
                            author_name = p.text.split(': ')[1]
                            res["author"]["name"] = author_name
                            res["author"]["email"] = ""
                        break
                has_meta = True
            if (not has_maintainers) and (title.text == "Maintainers"):
                maintainers = sb.find_all(
                    "span", {"class": "sidebar-section__maintainer"})
                for m in maintainers:
                    t = m.find(
                        "span", {"class": "sidebar-section__user-gravatar-text"})
                    res["maintainers"].append(t.text.strip(" \n"))
                has_maintainers = True
            if has_meta and has_maintainers:
                break
    except:
        logging.error(f"{name}")
        exit(0)
    return res


def single_package(name: str):
    soup = download_html(name)
    return parse_html(soup, name)


def run():
    if os.path.exists(DATA_PATH):
        return
    packages = list(pd.read_csv("data/package_statistics.csv")
                    ['package'].unique())
    p2am = dict()
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as inf:
            for line in inf:
                if '[INFO]' in line:
                    p, names = line.strip('\n').split('[INFO] ')[1].split(": ", 1)
                    p2am[p] = json.loads(names)
    logging.basicConfig(
        filename=LOG_PATH,
        filemode="a",
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO
    )
    remain_packages = set(packages) - set(list(p2am.keys()))
    print(f"{len(packages)} packages, finish {len(p2am)} packages, remain {len(remain_packages)} packages")
    for p in remain_packages:
        res = single_package(p)
        logging.info(f"{p}: {json.dumps(res)}")
        p2am[p] = res
    with open(DATA_PATH, 'w') as outf:
        json.dump(p2am, outf)


if __name__ == "__main__":
    run()
