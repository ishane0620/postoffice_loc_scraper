# from os import wait3
import os
import pandas as pd
import scrape_postoffice 
import get_loc_urls 
import asyncio
from typing import List 
# from playwright import async_playwright

ward_url = 'https://map.japanpost.jp/p/search/search.htm?&cond2=1&cond200=1&&&his=sa1&&type=ShopA&area1=01&area2=%A4%A2%A4%B5%A4%D2%A4%AB%A4%EF%A4%B7%23%23%B0%B0%C0%EE%BB%D4&slogflg=1&areaptn=1&selnm=%B0%B0%C0%EE%BB%D4'



async def get_df_of_ward(ward_url):
    # office_list = get_loc_urls.main(ward_url)
    office_list = get_loc_urls.extract_post_offices(get_loc_urls.fetch_soup(ward_url))
    df = pd.DataFrame(office_list)

    lats, lons = [],[]
    for loc in office_list:
        s = await scrape_postoffice.main(loc[1])
        lats.append(s[0][0])
        lons.append(s[0][1])
    df['Latitude'] = pd.DataFrame(lats)
    df['Longitude'] = pd.DataFrame(lons)

    return df

# res = asyncio.run(get_df_of_ward(ward_url))
# print(res.head())


def main():
    df = pd.read_csv('office_links.csv')
    
    # Create output directory if it doesn't exist
    output_dir = 'prefecture_loc_csvs'
    os.makedirs(output_dir,exist_ok = True)

    # Group by prefecture (ward_listing_url)
    grouped = df.groupby('ward_listing_url')
    
    for prefecture, group_df in grouped:
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