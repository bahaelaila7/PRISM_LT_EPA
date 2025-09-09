import pandas as pd
import glob
from collections import namedtuple, defaultdict
from pprint import pprint
import sqlite3

import re
pattern = re.compile(r'/([^/]+)/[^/]+/prism_([^_]+)_us_30s_(\d{4})(\d{2})?.parquet') #(var, year, month/None)

FileEntry = namedtuple('Entry',['filepath','mode','var','year','month'])
VARS=['ppt','tmin','tmean','tmax','vpdmin','vpdmax','tdmean']


if __name__ == '__main__':
    all_files = sorted(glob.glob(r'./output/*/*/*.parquet'))
    files = [FileEntry(file, *m.groups()) for file in all_files if ((m:=pattern.search(file)) and m.groups() is not None) ]
    assert len(files) == len(all_files)
    var_files = defaultdict(lambda: defaultdict(list))
    for ef in files:
        var_files[ef.var][ef.mode].append(ef)
    for VAR, mode_dict in var_files.items():
        for MODE, mode_files in mode_dict.items():
            print(VAR,MODE, mode_files[:2])
            table_name = f'{VAR}_{MODE}'
            con = None
            for ef in mode_files:
                print(VAR, MODE, ef.filepath)
                df = pd.read_parquet(ef.filepath)
                print(df.columns)
                if con is None:
                    con = sqlite3.connect("./prism_data.db")
                    con.execute(f'''DROP TABLE IF EXISTS {table_name}''')
                    sql= f'''CREATE TABLE IF NOT EXISTS {table_name}
                                    (POLYGON_ID INTEGER,
                                    EPA_L4_ID TEXT,
                                    EPA_L3_ID TEXT,
                                    YEAR INTEGER,
                                {'MONTH INTEGER,' if MODE == 'monthly' else ''}
                                    VALUE REAL,
                                    STD REAL,
                                    PXL_COUNT INT);
                                     '''
                    print(sql)
                    con.execute(sql)
                df.to_sql(table_name, con, if_exists='append',index=False)
            con.close()

""",
                                    UNIQUE(POLYGON_ID, 
                                        YEAR
                                {',MONTH' if MODE == 'monthly' else ''}
                                        )"""





