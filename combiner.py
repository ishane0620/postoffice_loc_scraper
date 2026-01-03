# from os import wait3
import pandas as pd
import scrape_postoffice 
import get_loc_urls 
import asyncio
from typing import List 
# from playwright import async_playwright

ward_url = 'https://map.japanpost.jp/p/search/search.htm?&cond2=1&cond200=1&&&his=sa1&&type=ShopA&area1=01&area2=%A4%A2%A4%B5%A4%D2%A4%AB%A4%EF%A4%B7%23%23%B0%B0%C0%EE%BB%D4&slogflg=1&areaptn=1&selnm=%B0%B0%C0%EE%BB%D4'



async def get_df_of_ward(ward_url, idx):
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


    
    df.to_csv(f'{idx}.csv') 
    return df

# res = asyncio.run(get_df_of_ward(ward_url))
# print(res.head())


def main():
    df = pd.read_csv('office_links.csv')
    for index, row in df.iterrows():
        ward_url = row['office_detail_url']
        prefecture = row['ward_listing_url']

        df2 = asyncio.run(get_df_of_ward(ward_url,index))
    df2.to_csv('office_links_with_coords.csv')

if __name__ == '__main__':
    main()