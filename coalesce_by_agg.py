import pandas as pd
import glob
from collections import namedtuple, defaultdict
from pprint import pprint
import sqlite3
from tqdm.auto import tqdm
import subprocess
import sys

import re
pattern = re.compile(r'/([^/]+)/[^/]+/prism_([^_]+)_us_30s_(\d{4})(\d{2})?.parquet') #(var, year, month/None)

FileEntry = namedtuple('Entry',['filepath','mode','var','year','month'])
VARS=['ppt','tmin','tmean','tmax','vpdmin','vpdmax','tdmean']


if __name__ == '__main__':
    output_file = 'prism_data_coalesced.db'
    all_files = sorted(glob.glob(r'./output/*/*/*.parquet'))
    files = [FileEntry(file, *m.groups()) for file in all_files if ((m:=pattern.search(file)) and m.groups() is not None) ]
    assert len(files) == len(all_files)
    year_files = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for ef in files:
        year_files[ef.year][ef.month][ef.mode].append(ef)
    print(subprocess.call(['rm','-rf',output_file]))
    # coalesce vars
    with sqlite3.connect(output_file) as conn:
        with tqdm(total = len(files)) as pbar:
            for YEAR, year_dict in year_files.items():
                for MONTH, month_dict in year_dict.items():
                    for MODE, mode_files in month_dict.items():
                        #pprint(mode_files)
                        df = None
                        for ef in mode_files:
                            #print("LOADING ", ef)
                            next_df = pd.read_parquet(ef.filepath)
                            next_df['YEAR'] = next_df['YEAR'].astype(int)
                            if MODE =='monthly':
                                next_df['MONTH'] = next_df['MONTH'].astype(int)
                            if df is None:
                                df = next_df.drop(['VALUE','STD'], axis=1)
                            else:
                                assert df['POLYGON_ID'].equals(next_df['POLYGON_ID'])
                            df[f'{ef.var}_mean'] = next_df['VALUE']
                            df[f'{ef.var}_std'] = next_df['STD']
                            pbar.update(1)
                        #print(df)
                        #sys.exit(0)
                        df.to_sql(MODE, conn, if_exists = 'append', index=False)
        print("DUMPING DATA DONE. CREATING INDICES...")
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS monthly_idx ON monthly(POLYGON_ID,YEAR,MONTH);')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS yearly_idx ON yearly(POLYGON_ID,YEAR);')
        print("DONE")
