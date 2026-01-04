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
PROGRESS_FILE = 'scraping_progress.json'

def save_progress(prefecture: str, completed_wards: list):
    """Save progress to JSON file."""
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            progress = {}
    
    progress[prefecture] = {
        'completed_wards': completed_wards,
        'last_updated': datetime.now().isoformat()
    }
    
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)

def load_progress() -> Dict:
    """Load progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {}
    return {}

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

    # Load previous progress
    progress = load_progress()

    # Group by prefecture (ward_listing_url)
    grouped = df.groupby('ward_listing_url')
    
    for prefecture, group_df in grouped:

        if prefecture in viewed:
            continue
        
        # Check if we have partial progress for this prefecture
        completed_wards = set()
        if prefecture in progress:
            completed_wards = set(progress[prefecture].get('completed_wards', []))
            print(f"Resuming {prefecture}: {len(completed_wards)} wards already completed")
        
        # Load existing CSV data if resuming
        safe_prefecture = prefecture.replace('/', '_').replace('\\', '_')
        output_path = os.path.join(output_dir, f'{safe_prefecture}.csv')
        existing_data = None
        if os.path.exists(output_path) and completed_wards:
            try:
                existing_data = pd.read_csv(output_path)
                print(f"Loaded {len(existing_data)} existing offices from {safe_prefecture}.csv")
            except Exception as e:
                print(f"Warning: Could not load existing CSV: {e}")
        
        # Combine all offices for this prefecture
        all_dfs = []
        
        for index, row in group_df.iterrows():
            ward_url = row['office_detail_url']
            
            # Skip if already completed
            if ward_url in completed_wards:
                print(f"Skipping already completed ward: {ward_url}")
                continue
            
            try:
                df_ward = asyncio.run(get_df_of_ward(ward_url))
                all_dfs.append(df_ward)
                
                # Mark as completed and save progress immediately
                completed_wards.add(ward_url)
                save_progress(prefecture, list(completed_wards))
                
                # Save incrementally to avoid data loss
                if all_dfs:
                    temp_df = pd.concat(all_dfs, ignore_index=True)
                    if existing_data is not None:
                        temp_df = pd.concat([existing_data, temp_df], ignore_index=True)
                        temp_df = temp_df.drop_duplicates(subset=[0, 1], keep='last')
                    temp_df.to_csv(output_path, index=False)
                
            except KeyboardInterrupt:
                print(f"\nInterrupted! Progress saved. Resume by running again.")
                save_progress(prefecture, list(completed_wards))
                raise
            except Exception as e:
                print(f"Error processing ward {ward_url}: {e}")
                # Continue with next ward even if one fails
                continue
        
        # Final save: Concatenate all dataframes for this prefecture
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            # Merge with existing data if resuming
            if existing_data is not None:
                combined_df = pd.concat([existing_data, combined_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=[0, 1], keep='last')  # Remove duplicates
            
            combined_df.to_csv(output_path, index=False)
            print(f"Saved {len(combined_df)} offices for {prefecture}")
        
        # Clear progress for completed prefecture
        if prefecture in progress:
            progress.pop(prefecture)
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    main()