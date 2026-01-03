# crawler_wards_to_offices.py
import time
import re
from typing import List, Set, Tuple, Optional, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Polite request headers: identify your scraper
HEADERS = {
    # Replace contact URL with your own site or email page
    "User-Agent": "Mozilla/5.0 (compatible; JapanPostCrawler/1.0; +https://example.com/contact)"
}
REQUEST_TIMEOUT = 20  # seconds
RATE_LIMIT_SECONDS = 0.8  # delay between requests
MAX_RETRIES = 3

WARDS = 'https://map.japanpost.jp/p/search/search.htm?radio=&type=ShopA&areaptn=1&tabno=1&cond101=&cond102=&cond200=1&cond2=1&youbi1=&timespan1=&type=ShopA&areaptn=1&tabno=2&cond103=&cond104=&cond200=1&youbi2=&timespan2=&type=ShopA&areaptn=1&tabno=3&cond105=&cond106=&cond200=1&youbi3=&timespan3=&type=ShopA&areaptn=1&tabno=4&cond107=&cond200=1&youbi4=&timespan4=&type=ShopA&areaptn=1&tabno=5&cond108=&cond109=&cond200=1&youbi5=&timespan5=&cond31='



WARD_URLS = {
    # Example placeholder formats — replace with actual links you get from the site’s search/filter
    # '北海道' : 'https://map.japanpost.jp/p/search/search.htm?&cond2=1&cond200=1&&&his=sa&&type=ShopA&area1=01&slogflg=1&areaptn=1&selnm=%CB%CC%B3%A4%C6%BB'
    # "https://map.japanpost.jp/p/search?q=%E4%B8%8A%E4%B8%8B%E5%8C%BA",  # (example)
}

def fetch_soup(url)-> Optional[BeautifulSoup]:
    for attempt in range(1,MAX_RETRIES+1):
        try:
            resp = requests.get(url, headers= HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                print(f'created beautiful soup')
                return BeautifulSoup(resp.text, 'html.parser')
            else:
                print(f'[WARN] {url} -> HTTP {resp.status_code}')
        except requests.RequestException as e:
            print(f"[ERROR] {url} attempt {attempt}/{MAX_RETRIES}: {e}")
        time.sleep(RATE_LIMIT_SECONDS * attempt)
    return None

def has_percent(url) -> bool:
    return bool(re.search(r'%', url))

def extract_ward_links(listing_soup: BeautifulSoup, base_url: str) -> List[str]:
    ward_links: List[str] = []
    for a in listing_soup.select('a[href]'):
        href= a.get('href')
        if not href:
            continue
        full = requests.compat.urljoin(base_url, href)

        if has_percent(full):
            ward_links.append(full)
            print(f'added ward detail')

    return list(dict.fromkeys(ward_links))

def extract_prefecture_links(listing_soup: BeautifulSoup, base_url: str) -> Dict[str,str]:
    prefecture_dict: Dict[str,str] = {}
    
    for a in listing_soup.select('a[href]'):
        href= a.get('href')
        prefecture_name = a.get_text(strip = True)

        if not href:
            continue
        full = requests.compat.urljoin(base_url, href)

        print(f'{prefecture_name} = {full}')
        if has_percent(href):
            if prefecture_dict.get(prefecture_name):
                print('[ERROR?] Duplicate Prefectures?')
                return
            prefecture_dict[prefecture_name] = full
            print(f'added pref {prefecture_name}\n\n\n\n\n')

    return prefecture_dict

def find_pagination_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links: List[str] = []
    for a in soup.select('a[href]'):
        text = a.get_text(strip=True)
        if text in ('次へ', '次', 'Next', '次のページ', '次>>', '>>'):
            href = a['href']
            links.append(requests.compat.urljoin(base_url, href))
            print(f'added pagination link from text: {text}, link: {href}')
        
    deduped = list(dict.fromkeys(links))
    return deduped

def crawl_prefectures(ward_url:str) -> List[str]:
    print(f'[INFO] Ward: {ward_url}')
    soup = fetch_soup(ward_url)
    if not soup:
        return []

    ward_links = extract_ward_links(soup,base_url=ward_url)

    visited : Set[str] = {ward_url}
    queue : List[str] = find_pagination_links(soup, base_url=ward_url)
    
    while queue:
        nxt = queue.pop(0)
        if nxt in visited:
            continue
            
        visited.add(nxt)
        print(f'[INFO] Ward Pagination: {nxt}')
        
        psoup = fetch_soup(nxt)
        if not psoup:
            continue
        ward_links += extract_ward_links(psoup, base_url=nxt)
        
        for l in find_pagination_links(psoup, base_url=nxt):
            if l not in visited and l not in queue:
                queue.append(l)
                
        time.sleep(RATE_LIMIT_SECONDS)

    return list(dict.fromkeys(ward_links))


def crawl_japan(japan_url:str) -> Dict[str,str]:
    print(f'[INFO] Japan, pref: {japan_url}')
    soup = fetch_soup(japan_url)
    if not soup:
        return []

    prefecture_links = extract_prefecture_links(soup, base_url=japan_url)

    visited : Set[str] = {japan_url}
    queue : List[str] = find_pagination_links(soup, base_url=japan_url)
    
    while queue:
        nxt = queue.pop(0)
        if nxt in visited:
            continue
            
        visited.add(nxt)
        print(f'[INFO] Prefecture Pagination: {nxt}')
        
        psoup = fetch_soup(nxt)
        if not psoup:
            continue
        
        prefecture_links.update(extract_prefecture_links(psoup, base_url=nxt))

        ###from here ---- down
        for l in find_pagination_links(psoup, base_url=nxt):
            if l not in visited and l not in queue:
                queue.append(l)
                
        time.sleep(RATE_LIMIT_SECONDS)
        
    return prefecture_links
        ### ------------


def main():
    

    #create wards
    wards = crawl_japan(WARDS)
    WARD_URLS = wards
    

    if not WARD_URLS:
        print('no ward_urls ERROR')
        return


    all_rows: List[Tuple[str,str]] = []
    
    for wurl in WARD_URLS:
        links = crawl_prefectures(WARD_URLS[wurl])
        for office_url in links:
            all_rows.append((wurl, office_url))
        time.sleep(RATE_LIMIT_SECONDS)
        
    print(f'all rows: {all_rows}')
    # Write to CSV for the next stage
    df = pd.DataFrame(all_rows, columns=["ward_listing_url", "office_detail_url"])
    output_file = "office_links.csv"
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"[INFO] Saved {len(df)} links to {output_file}")

if __name__ == "__main__":
    main()