import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://b.scsi.to/"  # For example, do whatever you want

source_code = requests.get(BASE_URL)
soup = BeautifulSoup(source_code.content, "lxml")
data = []  # put files in here
links = []

# Collect all links
for link in soup.find_all("a"):
    if link.get("href") != None:
        if (link.get("href").endswith("/")) and (
            link.get("href") != "../"
        ):  # if it looks like a file path, we'll add to links
            url = BASE_URL + link.get("href")
            links.append(url)
        elif link.get("href") != "../":  # If it looks like a file, we'll add to data
            url = BASE_URL + link.get("href")
            data.append(url)

while len(links) > 0:
    # the links list is essentially a list of unexplored
    # paths. we pop from it when we have fully
    # explored the file path.
    for link in links:
        source_code = requests.get(link)
        soup = BeautifulSoup(source_code.content, "lxml")
        for nested_link in soup.find_all("a"):
            if nested_link.get("href") != None:
                if (nested_link.get("href").endswith("/")) and (
                    nested_link.get("href") != "../"
                ):
                    url = link + nested_link.get("href")
                    links.append(url)
                elif nested_link.get("href") != "../":
                    url = link + nested_link.get("href")
                    data.append(url)
        links.remove(link)  # pop the link so that we know we're done with that path.


df = pd.DataFrame(data, columns=["url"])
df.to_csv("all_files_urls.csv", index=False)
