import pandas as pd
import os

print('a')


os.makedirs('prefecture_xslx', exist_ok=True)
for csv in os.listdir('prefecture_loc_csvs_cleaned'):
    print(f'cleaning {csv}')
    df = pd.read_csv(f'prefecture_loc_csvs_cleaned/{csv}')
    # Change file extension from .csv to .xlsx
    xlsx_filename = csv.replace('.csv', '.xlsx')
    df.to_excel(f'prefecture_xlsx/{xlsx_filename}', index=False)


