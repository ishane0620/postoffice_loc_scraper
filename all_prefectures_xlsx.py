import pandas as pd
import os
from googletrans import Translator


#Location ID	Resion 	地域	prefecture	都道府県	municipality	市区町村	asset_id	asset_name	建物名	postal_code	full_address	住所	Latitude	Longitude	asset_build_date	asset_floors
prefecture_map = {
    # 北海道地方
    "北海道": ["Hokkaido", "Hokkaido", "北海道"],

    # 東北地方
    "青森県": ["Aomori", "Tohoku", "東北"],
    "岩手県": ["Iwate", "Tohoku", "東北"],
    "宮城県": ["Miyagi", "Tohoku", "東北"],
    "秋田県": ["Akita", "Tohoku", "東北"],
    "山形県": ["Yamagata", "Tohoku", "東北"],
    "福島県": ["Fukushima", "Tohoku", "東北"],

    # 関東地方
    "茨城県": ["Ibaraki", "Kanto", "関東"],
    "栃木県": ["Tochigi", "Kanto", "関東"],
    "群馬県": ["Gunma", "Kanto", "関東"],
    "埼玉県": ["Saitama", "Kanto", "関東"],
    "千葉県": ["Chiba", "Kanto", "関東"],
    "東京都": ["Tokyo", "Kanto", "関東"],
    "神奈川県": ["Kanagawa", "Kanto", "関東"],

    # 中部地方（一般的には 北陸・甲信越・東海 の下位区分）
    # 北陸
    "新潟県": ["Niigata", "Chubu", "中部"],
    "富山県": ["Toyama", "Chubu", "中部"],
    "石川県": ["Ishikawa", "Chubu", "中部"],
    "福井県": ["Fukui", "Chubu", "中部"],
    # 甲信越
    "山梨県": ["Yamanashi", "Chubu", "中部"],
    "長野県": ["Nagano", "Chubu", "中部"],
    # 東海
    "岐阜県": ["Gifu", "Chubu", "中部"],
    "静岡県": ["Shizuoka", "Chubu", "中部"],
    "愛知県": ["Aichi", "Chubu", "中部"],
    "三重県": ["Mie", "Kansai", "関西"],  # 注: 三重県は行政・経済圏では東海に含まれることもありますが、地理区分では近畿（関西）に含まれるのが一般的

    # 関西（近畿）地方
    "滋賀県": ["Shiga", "Kansai", "関西"],
    "京都府": ["Kyoto", "Kansai", "関西"],
    "大阪府": ["Osaka", "Kansai", "関西"],
    "兵庫県": ["Hyogo", "Kansai", "関西"],
    "奈良県": ["Nara", "Kansai", "関西"],
    "和歌山県": ["Wakayama", "Kansai", "関西"],

    # 中国地方
    "鳥取県": ["Tottori", "Chugoku", "中国"],
    "島根県": ["Shimane", "Chugoku", "中国"],
    "岡山県": ["Okayama", "Chugoku", "中国"],
    "広島県": ["Hiroshima", "Chugoku", "中国"],
    "山口県": ["Yamaguchi", "Chugoku", "中国"],

    # 四国地方
    "徳島県": ["Tokushima", "Shikoku", "四国"],
    "香川県": ["Kagawa", "Shikoku", "四国"],
    "愛媛県": ["Ehime", "Shikoku", "四国"],
    "高知県": ["Kochi", "Shikoku", "四国"],

    # 九州地方（沖縄含む）
    "福岡県": ["Fukuoka", "Kyushu", "九州"],
    "佐賀県": ["Saga", "Kyushu", "九州"],
    "長崎県": ["Nagasaki", "Kyushu", "九州"],
    "熊本県": ["Kumamoto", "Kyushu", "九州"],
    "大分県": ["Oita", "Kyushu", "九州"],
    "宮崎県": ["Miyazaki", "Kyushu", "九州"],
    "鹿児島県": ["Kagoshima", "Kyushu", "九州"],
    "沖縄県": ["Okinawa", "Okinawa", "沖縄"],  # 地理区分としては九州地方に含める場合もありますが、ここでは独立させています
}

# address_parser.py

import re

# Prefecture names for identification
PREFECTURES = [
    "北海道",
    "東京都", "京都府", "大阪府",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県",
    "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県",
]

# Map common Japanese dash variants to ASCII hyphen
DASH_VARIANTS = {"−", "ー", "―", "‐", "─", "–"}

def normalize_dashes(text: str) -> str:
    for ch in DASH_VARIANTS:
        text = text.replace(ch, "-")
    return text

def normalize_digits(text: str) -> str:
    # Convert full-width digits to ASCII
    def to_ascii_digit(ch):
        code = ord(ch)
        if 0xFF10 <= code <= 0xFF19:  # full-width ０..９
            return chr(code - 0xFF10 + ord('0'))
        return ch
    return "".join(to_ascii_digit(c) for c in text)

def extract_postal_code_prefix(text: str):
    """
    Returns (postal_code_prefix, remainder)
    postal_code_prefix examples: '079-1134', '0791134'
    Accepts optional leading 〒 and spaces.
    """
    s = text.strip()
    # Normalize dashes in the entire input first
    s = normalize_dashes(s)
    # Regex: optional 〒, optional spaces, 3 digits, optional hyphen, 4 digits
    # m = re.match(r"^〒?\s*(\d*-\d*-?\d*?)\s*", s)
    m = re.match(r"^〒?\s*(\d{3}-\d{4}|\d{7})\s*", s)
    if m:
        code = m.group(1)
        remainder = s[m.end():].strip()
        return code, remainder
    else:
        print(f'postal code not found in {s}')
    return "", s

def find_prefecture(address: str):
    for pref in PREFECTURES:
        if address.startswith(pref):
            return pref
    return ""

def extract_city_ward_town(address_without_pref: str):
    """
    Extracts only 市 (city) and/or 区 (ward) from the address.
    - If both 市 and 区 exist, includes both (e.g., "横浜市青葉区")
    - If only 市 exists, includes just 市 (e.g., "武蔵野市")
    - If only 区 exists, includes just 区 (e.g., "中央区")
    - If neither exists, returns None
    Does NOT include 町 (town) or 村 (village).
    """
    # Find the last occurrence of either 市 or 区
    last_shi_pos = address_without_pref.rfind('市')
    last_ku_pos = address_without_pref.rfind('区')
    # last_chou_pos = address_without_pref.rfind('町')
    # last_mura_pos = address_without_pref.rfind('村')
    # Determine the cut position (end of the last 市 or 区 found)
    cut_idx = None
    if last_shi_pos != -1 and last_ku_pos != -1:
        # Both exist, take up to the last one
        cut_idx = max(last_shi_pos, last_ku_pos) + 1
    elif last_shi_pos != -1:
        # Only 市 exists
        cut_idx = last_shi_pos + 1
    elif last_ku_pos != -1:
        # Only 区 exists
        cut_idx = last_ku_pos + 1
    else:
        # Neither exists
        return None
    
    # Extract from start up to and including the last 市 or 区
    return address_without_pref[:cut_idx].strip()

def extract_block_numbers(address_tail: str):
    """
    Extracts the block numbers sequence like '1-3-7'.
    Accept variants with full-width digits and Japanese dash variants.
    """
    normalized = normalize_digits(normalize_dashes(address_tail))
    m = re.search(r"\b(\d+\s*-\s*\d+(?:\s*-\s*\d+)?)\b", normalized)
    if m:
        # Return normalized ASCII hyphen + ASCII digits
        return m.group(1).replace(" ", "")
    return ""

# def split_japanese_address(full_address):
#     """
#     Input: string or pandas Series
#     - If string: e.g. '079-1134 北海道赤平市泉町１−３−７'
#     - If Series: applies function to each element
#     Output:
#     - If string: dict with "住所", "市区町村", "postal_code"
#     - If Series: DataFrame with columns "住所", "市区町村", "postal_code"
#     """
#     # If it's a Series, apply the function to each element
#     if isinstance(full_address, pd.Series):
#         results = full_address.apply(split_japanese_address)
#         # Convert list of dicts to DataFrame
#         return pd.DataFrame({
#             "住所": [r["住所"] for r in results],
#             "市区町村": [r["市区町村"] for r in results],
#             "postal_code": [r["postal_code"] for r in results]
#         })
    
#     # Original function logic for strings
#     # Step 1: normalize dash variants
#     s = normalize_dashes(full_address.strip())

#     # Step 2: extract postal code prefix
#     zip_prefix, remainder = extract_postal_code_prefix(s)

#     # Step 3: normalize digits to ASCII for consistent output
#     remainder_ascii = normalize_digits(remainder)

#     # Ensure ASCII hyphens throughout
#     remainder_ascii = normalize_dashes(remainder_ascii)

#     # 住所 (no postal code, normalized)
#     jusho = remainder_ascii

#     # Step 4: find prefecture and city/town/ward + subarea
#     pref = find_prefecture(remainder_ascii)
#     rest = remainder_ascii[len(pref):] if pref else remainder_ascii

#     # City + subarea up to number block
#     city_subarea = extract_city_ward_town(rest)

#     # Step 5: block numbers (for reference, but not used as postal_code)
#     if city_subarea is None:
#         block_numbers = extract_block_numbers(rest)
#     else:
#         block_numbers = extract_block_numbers(rest[len(city_subarea):])

#     # Final normalization: replace any lingering Japanese dashes
#     jusho = normalize_dashes(jusho)

#     return {
#         "住所": jusho,
#         "市区町村": city_subarea,
#         "postal_code": zip_prefix,  # Use the actual postal code prefix, not block numbers
#     }

def split_japanese_address(full_address):
    """
    Input: string or pandas Series
    - If string: e.g. '079-1134 北海道赤平市泉町１−３−７'
    - If Series: applies function to each element
    Output:
    - If string: dict with "住所", "市区町村", "postal_code"
    - If Series: DataFrame with columns "住所", "市区町村", "postal_code"
    """
    # If it's a Series, apply the function to each element
    if isinstance(full_address, pd.Series):
        results = full_address.apply(split_japanese_address)
        # Convert list of dicts to DataFrame
        return pd.DataFrame({
            # "住所": [r["住所"] for r in results],
            # "市区町村": [r["市区町村"] for r in results],
            "postal_code": [r["postal_code"] for r in results]
        })
    
    # Original function logic for strings
    # Step 1: normalize dash variants
    s = normalize_dashes(full_address.strip())

    # Step 2: extract postal code prefix
    zip_prefix, remainder = extract_postal_code_prefix(s)
    return {'postal_code': zip_prefix}


import asyncio
import time
import random
import json
from datetime import datetime

def log_translation_error(jap_text, error):
    """Log translation errors to translation_errors.json"""
    error_entry = {
        "timestamp": datetime.now().isoformat(),
        "japanese_text": jap_text,
        "error_type": type(error).__name__,
        "error_message": str(error)
    }
    
    # Read existing errors or create new list
    try:
        with open('translation_errors.json', 'r', encoding='utf-8') as f:
            errors = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        errors = []
    
    # Append new error
    errors.append(error_entry)
    
    # Write back to file
    with open('translation_errors.json', 'w', encoding='utf-8') as f:
        json.dump(errors, f, indent=2, ensure_ascii=False)

async def translate_jap_to_eng_async(jap_str, max_retries=3):
    """Async helper for translation with retry logic"""
    translator = Translator()
    
    for attempt in range(max_retries):
        try:
            result = await translator.translate(jap_str, src='ja', dest='en')
            return result.text
        except Exception as e:
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
                continue
            else:
                raise e

def translate_jap_to_eng(jap):
    """
    Translates Japanese text to English using Google Translate.
    Handles both strings and pandas Series.
    Includes retry logic and delays to handle rate limiting.
    """
    if isinstance(jap, pd.Series):
        translated_list = []
        for i, text in enumerate(jap):
            # Add delay between items to avoid rate limiting
            if i > 0:
                time.sleep(0.2)  # 200ms delay between translations
            
            result = translate_jap_to_eng(text)
            translated_list.append(result)
        
        return pd.Series(translated_list, index=jap.index)
    
    try:
        if pd.isna(jap):
            return None
        
        jap_str = str(jap)
        if jap_str == '' or jap_str == 'nan':
            return None
        
        # Run async translation with retry logic
        result = asyncio.run(translate_jap_to_eng_async(jap_str))
        print(f'ran translation, result: {result}')
        return result
    except Exception as e:
        # Log to JSON file after all retries have failed
        log_translation_error(jap, e)
        return None




def extract_postal_code(df_master, prefecture_xlsx_name):
    """
    Extracts the postal code from the address and adds it to the master_df.
    """
    df_prefecture = pd.read_excel(os.path.join('prefecture_xlsx', prefecture_xlsx_name))
    df_prefecture = df_prefecture.drop(columns=['0','Latitude','Longitude']).rename(columns={'1':'website (for verification)', '2':'postal_code'})
    df_prefecture['postal_code_new'] = split_japanese_address(df_prefecture['postal_code'])['postal_code']
    
    # Merge with the new postal code column (include the merge key column)
    df_master = df_master.merge(df_prefecture[['website (for verification)', 'postal_code_new']], 
                                on='website (for verification)', how='left')
    
    # Update postal_code where we have new values (fillna to preserve existing values)
    df_master['postal_code'] = df_master['postal_code'].fillna(df_master['postal_code_new'])
    df_master.drop(columns=['postal_code_new'], inplace=True)
    
    return df_master

# main_df = pd.DataFrame(columns=['Location ID', 'Region', '地域', 'prefecture', '都道府県', 'municipality', '市区町村', 'asset_id', 'asset_name', '建物名', 'postal_code', 'full_address', '住所', 'Latitude', 'Longitude', 'asset_build_date', 'asset_floors', 'website (for verification)'])


# for xlsx in os.listdir('prefecture_xlsx'):
#     # if xlsx == '北海道.xlsx':
#     df = pd.read_excel(os.path.join('prefecture_xlsx', xlsx))
#     prefecture_jp = xlsx.replace('.xlsx', '')
#     triplet = prefecture_map[prefecture_jp]
#     df['都道府県'] = prefecture_jp
#     df['prefecture'] = triplet[0]
#     df ['Region'] = triplet[1]
#     df['地域'] = triplet[2]
#     df.rename(columns={
#         '0': '建物名',
#         '1' : 'website (for verification)'
#     }, inplace = True)
#     df['Location ID'] = None
    
#     split_address = split_japanese_address(df['2'])
#     df['住所'] = split_address["住所"]
#     df['市区町村'] = split_address["市区町村"]
#     df['postal_code'] = split_address["postal_code"]

#     df['asset_name'] = translate_jap_to_eng(df['建物名'])
#     df['municipality'] = translate_jap_to_eng(df['市区町村'])
#     df['full_address'] = translate_jap_to_eng(df['住所'])

#     main_df = pd.concat([main_df, df[[col for col in main_df.columns if col in df.columns]]])    # print(translate_jap_to_eng(df['建物名']))


# os.makedirs('final_folder',exist_ok = True)
# main_df.to_excel(f'final_folder/all_prefectures.xlsx', index=False)
# ['municipality', 'asset_id', 'asset_name', 'full_address', 'asset_build_date', 'asset_floors']


df_master = pd.read_excel('final_folder/all_prefectures1.xlsx')
print(df_master[['postal_code']])
print(f'before shape: {df_master.shape}')
print(f'before na postal codes: {df_master['postal_code'].isna().sum()}')

df_master.drop(columns=['postal_code'], inplace=True)
df_master['postal_code'] = None
for xlsx in os.listdir('prefecture_xlsx'):
    df_master = extract_postal_code(df_master, xlsx)
    print(f'added {xlsx}, shape: {df_master.shape}')

print(f'after shape: {df_master.shape}')
print(f'after na postal codes: {df_master['postal_code'].isna().sum()}')

# Reorder columns to match the desired order
desired_column_order = ['Location ID', 'Region', '地域', 'prefecture', '都道府県', 'municipality', '市区町村', 'asset_id', 'asset_name', '建物名', 'postal_code', 'full_address', '住所', 'Latitude', 'Longitude', 'asset_build_date', 'asset_floors', 'website (for verification)']
# Only reorder columns that exist in the DataFrame
existing_columns = [col for col in desired_column_order if col in df_master.columns]
# Add any remaining columns that weren't in the desired order
remaining_columns = [col for col in df_master.columns if col not in desired_column_order]
df_master = df_master[existing_columns + remaining_columns]

print(df_master[['postal_code']].head(50))

print(df_master.columns)