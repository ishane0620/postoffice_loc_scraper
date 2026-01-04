# from os import wait3
import os
import json
import pandas as pd
import scrape_postoffice 
import get_loc_urls 
import asyncio
from datetime import datetime
from typing import List, Dict
# from playwright import async_playwright

ward_url = 'https://map.japanpost.jp/p/search/search.htm?&cond2=1&cond200=1&&&his=sa1&&type=ShopA&area1=01&area2=%A4%A2%A4%B5%A4%D2%A4%AB%A4%EF%A4%B7%23%23%B0%B0%C0%EE%BB%D4&slogflg=1&areaptn=1&selnm=%B0%B0%C0%EE%BB%D4'

ERROR_LOG_FILE = 'scraping_errors.json'

def log_error_to_json(error_data: Dict):
    """Append error to JSON log file."""
    # Load existing errors if file exists
    errors = []
    if os.path.exists(ERROR_LOG_FILE):
        try:
            with open(ERROR_LOG_FILE, 'r', encoding='utf-8') as f:
                errors = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            errors = []
    
    # Add timestamp if not present
    if 'timestamp' not in error_data:
        error_data['timestamp'] = datetime.now().isoformat()
    
    # Append new error
    errors.append(error_data)
    
    # Write back to file
    with open(ERROR_LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(errors, f, indent=2, ensure_ascii=False)


async def get_df_of_ward(ward_url):
    # office_list = get_loc_urls.main(ward_url)
    office_list = get_loc_urls.extract_post_offices(get_loc_urls.fetch_soup(ward_url))
    df = pd.DataFrame(office_list)

    lats, lons = [],[]
    for i, loc in enumerate(office_list):
        # Add delay between requests to avoid network suspension
        if i > 0:
            await asyncio.sleep(0.5)  # 500ms delay between requests
        
        # Retry logic for network errors
        max_retries = 5
        for attempt in range(max_retries):
            try:
                s = await scrape_postoffice.main(loc[1])
                if s and s[0] and isinstance(s[0], tuple) and len(s[0]) == 2:
                    lats.append(s[0][0])
                    lons.append(s[0][1])
                    break
                else:
                    # If coordinates are None, append None values
                    lats.append(None)
                    lons.append(None)
                    break
            except Exception as e:
                
                
                if attempt < max_retries - 1:
                    print(f"Error scraping {loc[1]}, attempt {attempt + 1}/{max_retries}: {e}")
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                else:

                    error_info = {
                    'url': loc[1],
                    'office_name': loc[0] if len(loc) > 0 else 'Unknown',
                    'attempt': attempt + 1,
                    'max_retries': max_retries,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'ward_url': ward_url
                    }


                    print(f"Failed to scrape {loc[1]} after {max_retries} attempts: {e}")
                    error_info['status'] = 'failed'
                    log_error_to_json(error_info)
                    lats.append(None)
                    lons.append(None)
    
    df['Latitude'] = lats
    df['Longitude'] = lons

    return df

# res = asyncio.run(get_df_of_ward(ward_url))
# print(res.head())


def main():

    viewed = ['三重県(429)','京都府(463)', '佐賀県(199)', '兵庫県(949)', '北海道(1443)']
     
    df = pd.read_csv('office_links.csv')
    
    # Create output directory if it doesn't exist
    output_dir = 'prefecture_loc_csvs'
    os.makedirs(output_dir,exist_ok = True)

    # Group by prefecture (ward_listing_url)
    grouped = df.groupby('ward_listing_url')
    
    for prefecture, group_df in grouped:

        if prefecture in viewed:
            continue
        # Combine all offices for this prefecture
        all_dfs = []
        
        for index, row in group_df.iterrows():
            ward_url = row['office_detail_url']
            df_ward = asyncio.run(get_df_of_ward(ward_url))
            all_dfs.append(df_ward)
        
        # Concatenate all dataframes for this prefecture
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save one CSV file per prefecture
        # Sanitize filename by removing invalid characters
        safe_prefecture = prefecture.replace('/', '_').replace('\\', '_')
        output_path = os.path.join(output_dir, f'{safe_prefecture}.csv')
        combined_df.to_csv(output_path, index=False)

if __name__ == '__main__':
    main()