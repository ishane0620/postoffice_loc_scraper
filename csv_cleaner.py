import os 
import pandas as pd
import re

new_folder = 'prefecture_loc_csvs_cleaned'
os.makedirs(new_folder, exist_ok=True)



for file in os.listdir('prefecture_loc_csvs'):
    new_file_name = re.sub(r'\(\d+\)', '', file)
    df = pd.read_csv(f'prefecture_loc_csvs/{file}')
    print(f'{file} has {df.isna().sum().sum()} missing values')
    duplicate_indices = df.index[df.duplicated()].tolist()
    if duplicate_indices:
        print(f'{file} has {len(duplicate_indices)} duplicates at indices: {duplicate_indices}')
    else:
        print(f'{file} has no duplicates')


    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df.to_csv(f'prefecture_loc_csvs_cleaned/{new_file_name}', index=False)

