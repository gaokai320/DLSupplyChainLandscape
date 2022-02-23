import json
import logging
import requests
from bs4 import BeautifulSoup


def github_dependents(pkg:str, url: str):
    headers = {"Authorization": "token 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"}
    request_url = f"https://{url}/network/dependents?dependent_type=REPOSITORY"
    try: 
        dependent_info = requests.get(
            request_url
        )
        print(dependent_info.ok)
    except json.decoder.JSONDecodeError:
        logging.error(f"{pkg}: {url} does not exist")
        return
    print(dependent_info.content)


if __name__ == "__main__":
    github_dependents("geniverse", "github.com/thegeniverse/geniverse")
