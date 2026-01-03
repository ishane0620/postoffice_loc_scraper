# filename: jp_post_dom_dump.py
import asyncio
from playwright.async_api import async_playwright
import json 
import re
from typing import Optional, Tuple, Dict, List
import unicodedata
import sys

TARGET_URL = "https://map.japanpost.jp/p/search/dtl/300170022000/?&cond2=1&cond200=1&&his=sa2"

EMAP_RE = re.compile(
    r"ZdcEmapInit\s*\(\s*'([+-]?\d+(?:\.\d+)?)'\s*,\s*'([+-]?\d+(?:\.\d+)?)'",
    re.IGNORECASE
)

def extract_coords(html: str) -> Optional[Tuple[float, float]]:
    """
    Search the HTML/JS text for ZdcEmapInit(...) and return (lat, lng) as floats.
    Returns None if not found.
    """
    m = EMAP_RE.search(html)
    if not m:
        return None
    lat_str, lng_str = m.group(1), m.group(2)
    try:
        return float(lat_str), float(lng_str)
    except ValueError:
        return None

async def main(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Go to page and wait for network/render to settle
        await page.goto(url, wait_until="networkidle")

        # Optional: wait for a known selector that contains the address
        # Replace with a selector you observe in DevTools
        try:
            await page.wait_for_selector("text=〒", timeout=5000)
        except:
            pass

        # Dump the live DOM (post-JS), this mirrors DevTools Elements view
        html = await page.content()
        coords = (extract_coords(html))
        postal = (extract_postcode_after_escape(html))


        # with open('output.json', 'w') as f:
        #     json.dump(html, f, indent=2)
        # Example: extract lat/lng if present in URL or scripts
        # url = page.url
        # print(f"Final URL: {url}")

        # If coordinates are embedded in JS variables or data-* attributes,
        # query them directly via DOM:
        # example: find any element with data-lat / data-lng attributes
        

        await browser.close()
        print([coords, postal])
        return [coords, postal]


POSTCODE_AFTER_ESC_RE = re.compile(
    r"""〒        # literal \u3012 escape (postal mark)
        \s*              # optional whitespace (tabs/spaces)
        (\d{3}-?\d{4})   # capture 3 digits, optional hyphen, 4 digits
    """,
    re.VERBOSE
)

def extract_postcode_after_escape(html: str) -> Optional[str]:
    """
    Extract the Japanese postcode that appears right after the escaped postal mark \\u3012
    from the full HTML string. Returns the normalized 7-digit string (without hyphen),
    or None if not found.
    """
    m = POSTCODE_AFTER_ESC_RE.search(html)
    if not m:
        return "error regex not found"
    raw = m.group(1)
    return raw


if __name__ == "__main__":

    if len(sys.argv) > 1:
        url = sys.argv[1]
    else: 
        url = TARGET_URL

    result = asyncio.run(main(url))

