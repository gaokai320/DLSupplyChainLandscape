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

def repo_url(package: str):
    print(package)
    try:
        pypi_info = requests.get(
            f"https://pypi.org/pypi/{package}/json"
        ).json()['info']
    except json.decoder.JSONDecodeError:
        logging.error(f"{package} does not exist on PyPI")
        return ''
    home_page, project_urls = pypi_info.get('home_page', ''), pypi_info.get("project_urls", [])
    m = URL_PATTERN.search(home_page)
    if m:
        return m.group(0)
    for pu in project_urls.values():
        m = URL_PATTERN.search(pu)
        if m:
            return m.group(0)
    return ''

if __name__ == "__main__":
    print(repo_url("tensorflow"))
    print(repo_url("torch"))
    print(repo_url("mindspore"))
