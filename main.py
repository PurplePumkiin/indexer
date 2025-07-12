import os
import requests
import tarfile
from bs4 import BeautifulSoup
from tinydb import TinyDB, Query
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

filesPerDataset = 2500  # Number of files per dataset
db = TinyDB('crawler-db.json')
Page = Query()

def save_html(content, url):
    parsed = urlparse(url)
    safe_path = parsed.netloc.replace('.', '_') + parsed.path.replace('/', '_')
    if not safe_path.endswith('.html'):
        safe_path += '.html'
    full_path = os.path.join("temp-data", safe_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)
    return full_path

def clearTempFiles():
    directory = "temp-data"
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    return list({urljoin(base_url, a.get('href')) for a in soup.find_all('a', href=True)})

def strip_query(url):
    parsed = urlparse(url)
    stripped = parsed._replace(query='', fragment='')
    return urlunparse(stripped)

def crawl(url):
    url = strip_query(url)
    if db.contains(Page.url == url):
        print(f"Already crawled: {url}")
        return []

    try:
        response = requests.get(url, timeout=5)
        status = response.status_code
        content = response.text
        links = extract_links(content, url)
        saved_path = save_html(content, url)
        timestamp = datetime.utcnow().isoformat()

        db.insert({
            'url': url,
            'status': status,
            'links': links,
            'saved_path': saved_path,
            'timestamp': timestamp
        })

        print(f"Crawled: {url} ({status})")
        return links
    except Exception as e:
        print(f"Failed to crawl {url}: {e}")
        db.insert({
            'url': url,
            'status': None,
            'links': [],
            'saved_path': None,
            'timestamp': datetime.utcnow().isoformat()
        })
        return []

# Example crawl queue
to_crawl = ['https://www.foxnews.com/']
crawled = set()
sitesCrawled = 0
dataset = '1'

while to_crawl:
    current = to_crawl.pop(0)
    if current not in crawled:
        new_links = crawl(current)
        sitesCrawled += 1
        if sitesCrawled == filesPerDataset:

            with tarfile.open(f'data/dataset{dataset}.tar.gz', 'w:gz') as tar:
                for root, dirs, files in os.walk('temp-data'):
                    for file in files:
                        full_path = os.path.join(root, file)
                        tar.add(full_path, arcname=os.path.relpath(full_path, 'temp-data'))
            print(f"Archive {dataset} created")
            dataset = str(int(dataset) + 1)
            clearTempFiles()
            sitesCrawled = 0

        to_crawl.extend([link for link in new_links if link not in crawled])
        crawled.add(current)