# crawler_wards_to_offices.py
import time
import re
from typing import List, Set, Tuple, Optional, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd

### gets all names, urls, address of postoffices on ward page


# Polite request headers: identify your scraper
HEADERS = {
    # Replace contact URL with your own site or email page
    "User-Agent": "Mozilla/5.0 (compatible; JapanPostCrawler/1.0; +https://example.com/contact)"
}
REQUEST_TIMEOUT = 15  # seconds
RATE_LIMIT_SECONDS = 0.5  # delay between requests
MAX_RETRIES = 3

WARD = 'https://map.japanpost.jp/p/search/search.htm?&cond2=1&cond200=1&&&his=sa%2Csa1&&type=ShopA&area1=47&area2=%A4%A6%A4%E9%A4%BD%A4%A8%A4%B7%23%23%B1%BA%C5%BA%BB%D4&slogflg=1&areaptn=1&selnm=%B1%BA%C5%BA%BB%D4'



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
                # print(f'created beautiful soup')
                return BeautifulSoup(resp.text, 'html.parser')
            else:
                print(f'[WARN] {url} -> HTTP {resp.status_code}')
        except requests.RequestException as e:
            print(f"[ERROR] {url} attempt {attempt}/{MAX_RETRIES}: {e}")
        time.sleep(RATE_LIMIT_SECONDS * attempt)
    return None

def extract_post_offices(soup: BeautifulSoup) -> List[List[str]]:
    """
    Extract post office information from the listing page.
    Returns a list of [name, url, address] for each post office.
    """
    results = []
    
    # Find all table rows that contain post office data
    rows = soup.select('tr')
    
    for row in rows:
        # Find the name cell (contains the link with post office name)
        name_cell = row.select_one('td.searchShopListDataNm')
        if not name_cell:
            continue
            
        # Extract the link
        link = name_cell.select_one('a[href]')
        if not link:
            continue
            
        # Get the URL (handle HTML entities like &amp;)
        url = link.get('href', '').replace('&amp;', '&')
        
        # Get the name - it's in the <p> tag, but we need to extract text after the <img>
        name_p = link.select_one('p')
        if name_p:
            # Get all text, which will include the name after the image
            name = name_p.get_text(strip=True)
            # Remove any leading/trailing whitespace
            name = name.strip()
        else:
            name = link.get_text(strip=True)
        
        # Find the address cell
        address_cell = row.select_one('td.searchShopListDataDt')
        if not address_cell:
            continue
            
        # Extract address from the nested table
        address_td = address_cell.select_one('td')
        if address_td:
            # Get text and replace <br/> with space, then clean up
            address = address_td.get_text(separator=' ', strip=True).strip('〒')
            # Clean up multiple spaces
            address = ' '.join(address.split())
        else:
            address = ''
        
        # Only add if we have all three pieces of information
        if name and url and address:
            results.append([name, url, address])
    print(results)
    return results

def main(WARD):
    return extract_post_offices(fetch_soup(WARD))


if __name__ == '__main__':
    main(WARD)