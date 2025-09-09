import rasterio as rx
import rasterstats as rs
import sqlite3
import geopandas as gpd
import sys
import glob
import re
import subprocess
import pandas as pd
import pandasql as pdsql
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm.auto import tqdm
from pprint import pprint
import os

EPA_SHAPEFILE = '../EPA_ECOREGIONS/l4_vector.shp'
DATA_COLUMNS = ["POLYGON_ID", "EPA_L4_ID", "EPA_L3_ID", "YEAR", "VALUE", "STD", "PXL_COUNT", "MONTH",]

GDF = None

def process_file(filepath, var, year, month = None):
    global GDF
    unzip_path = f'{filepath}_'
    filename = filepath.split('/')[-1]
    basename = filename[:-4]
    tif_path = f"{unzip_path}/{basename}.tif"
    output_dir = f'./output/{"yearly" if month is None else "monthly"}/{var}'
    output_path = f'{output_dir}/{basename}.parquet'
    if os.path.exists(output_path):
        # already processed
        return

    unzip_res =subprocess.call(['unzip','-qqod',unzip_path,filepath])

    with rx.open(tif_path) as src:
        data =src.read(1)
        nodata = src.nodata
        crs = src.crs
        transform = src.transform
        transform_gdal = transform.to_gdal()
    if GDF is None:
        #print("LOADING GDF")
        GDF = gpd.read_file(EPA_SHAPEFILE)#.sort_values()
        if GDF.crs != crs:
            GDF = GDF.to_crs(crs)
    zonal_stats = rs.zonal_stats(GDF, data, transform= transform_gdal, nodata=nodata, stats="mean std count")
    assert len(GDF['id_str']) == len(zonal_stats)
    # deleting unzipped
    rm_res = subprocess.call(['rm','-rf',unzip_path])
    data = [(polygon_index,
                       id_str,
                       id_str[:id_str.rfind('.')],
                       year,
                       stats['mean'],
                       stats['std'],
                       stats['count'])
        for polygon_index,(id_str, stats) in enumerate(zip(GDF['id_str'],zonal_stats))]

    data_df = pd.DataFrame(data , columns = DATA_COLUMNS[:-1])
    if month is not None:
        data_df[DATA_COLUMNS[-1]] = month

    # mkdir -p
    os.makedirs(output_dir, exist_ok=True)

    # dumping data into parquet file
    data_df.to_parquet(output_path)


def parallel_process(entries, max_workers = 30):
    with ProcessPoolExecutor(max_workers= max_workers) as ex:
        tasks = [ex.submit(process_file,*args) for args in entries]
        with tqdm(total=len(tasks)) as pbar:
            for res in as_completed(tasks):
                res.result()
                pbar.update(1)


if __name__ == '__main__':

    zipfile_pattern = re.compile(r'prism_([^_]+)_us_30s_(\d{4})(\d{2})?.zip$')
    get_filepath_var_year_month = lambda filepath: m.groups() if (m :=zipfile_pattern.search(filepath)) else None
    all_files = [(file,get_filepath_var_year_month(file)) for file in sorted(glob.glob(f"./800m/*/monthly/*/*.zip"))]
    files = [(f,*pf) for f,pf in all_files if pf] # (filepath, var, year, month)
    assert len(files) == len(all_files),  [(f,*pf) for f,pf in all_files if pf is None] # (filepath, var, year, month)
    #filtered_files = (filepath for filepath in files if 
    monthly_entries = [(f,var, year, month) for f,var, year, month in files if month is not None] # (filepath, var, year, month)
    yearly_entries = [(f,var, year) for f,var, year, month in files if month is None] # (filepath, var, year)


    print("PROCESSING YEARLY")
    parallel_process(yearly_entries)

    print("PROCESSING MONTHLY")
    parallel_process(monthly_entries)










"""
sys.exit(0)
VARS=['ppt','tmin','tmean','tmax','vpdmin','vpdmax','tdmean']
#VAR='ppt'


for VAR in VARS:
    RASTERS_PREFIX =f"./800m/{VAR}/monthly/"
    gdf = gpd.read_file(EPA_SHAPEFILE)

    #print(len(gdf['id_str']),len(gdf['id_str'].unique()))
    #sys.exit(0)
    #print(gdf)
    #print(gdf.columns)
    #for _,row in gdf.iterrows():
    #    print(row)
    #    break

    files = (get_filepath_year_month(file) for file in sorted(glob.glob(f"{RASTERS_PREFIX}/*/*.zip")))
    #filtered_files = (filepath for filepath in files if 
    entries = [f for f in files if f] # (filepath, year, month)


    with sqlite3.connect("./prism_data.db") as con:
        cur = con.cursor()
        con.execute(f'''DROP TABLE IF EXISTS {VAR}''')
        con.execute(f'''CREATE TABLE IF NOT EXISTS {VAR}
                        (POLYGON_ID INTEGER,
                        EPA_L4_ID TEXT,
                        EPA_L3_ID TEXT,
                        YEAR INTEGER,
                        MONTH INTEGER,
                        VALUE REAL,
                        STD REAL,
                        PXL_COUNT INT,
                        UNIQUE(POLYGON_ID, YEAR, MONTH));
                         ''')
        done_polygons = set(con.execute(f'''SELECT distinct year, month FROM {VAR}'''))
        '''
        print(done_polygons)

        print(len(gdf))
        # remove polygons from gpd
        gdf = gdf[~(gdf['id_str'].isin(done_polygons))]
        print(len(gdf))
        '''
        for filepath, year, month in entries:
            year = int(year)
            month = int(month)
            unzip_res =subprocess.call(['unzip','-od',f'{filepath}_',filepath])
            tif_path = f"{filepath}_/{filepath.split('/')[-1][:-3]}tif"
            print(tif_path)
            with rx.open(tif_path) as src:
                data =src.read(1)
                nodata = src.nodata
                crs = src.crs
                transform = src.transform
                transform_gdal = transform.to_gdal()
            '''
            print(data)
            print(data.shape)
            print(nodata)
            print(transform, type(transform))
            print(transform_gdal, type(transform_gdal))
            print(*transform)
            print(*transform_gdal)
            '''
            if gdf.crs != crs:
                gdf = gdf.to_crs(crs)

            zonal_stats = rs.zonal_stats(gdf, data, transform= transform_gdal, nodata=nodata, stats="mean std count")
            assert len(gdf['id_str']) == len(zonal_stats)
            rm_res = subprocess.call(['rm','-rf',f'{filepath}_'])
            data_to_insert = [(polygon_index,
                               id_str,
                               id_str[:id_str.rfind('.')],
                               year,
                               month,
                               stats['mean'],
                               stats['std'],
                               stats['count'])
                for polygon_index,(id_str, stats) in enumerate(zip(gdf['id_str'],zonal_stats))]
            #pd.DataFrame(data_to_insert, columns=['POLYGON_INDEX','EPA_L4_ID','EPA_L3_ID', 'YEAR', 'MONTH', 'VALUE', 'STD', 'PXL_COUNT'])
            try:
                con.execute("BEGIN TRANSACTION;")
                cur.executemany(f'''INSERT INTO {VAR}(POLYGON_ID, EPA_L4_ID, EPA_L3_ID, YEAR, MONTH, VALUE, STD, PXL_COUNT)

                                VALUES(?, ?, ?, ?, ?, ?, ?, ?)''', data_to_insert)
                con.commit()
            except sqlite3.Error as e:
                print(f"Error inserting into sql at {year},{month}: {e}")
                con.rollback()
                sys.exit(0)



"""

