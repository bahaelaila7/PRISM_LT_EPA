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
from pathlib import Path
import argparse
from pprint import pprint
import os

#EPA_SHAPEFILE = './EPA_ECOREGIONS/l4_vector.shp'
DATA_COLUMNS = ["POLYGON_ID", "EPA_L4_ID", "EPA_L3_ID", "YEAR", "VALUE", "STD", "PXL_COUNT", "MONTH",]

GDF = None

def process_file(epa_shapefile, output_basedir, filepath, var, year, month = None):
    global GDF
    unzip_path = f'{filepath}_'
    filename = filepath.split('/')[-1]
    basename = filename[:-4]
    tif_path = f"{unzip_path}/{basename}.tif"
    output_dir = f'{output_basedir}/{"yearly" if month is None else "monthly"}/{var}'
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
        GDF = gpd.read_file(epa_shapefile)#.sort_values()
        if GDF.crs != crs:
            GDF = GDF.to_crs(crs)
    zonal_stats = rs.zonal_stats(GDF, data, transform= transform_gdal, nodata=nodata, stats="mean std count")
    assert len(GDF['EPA_L4_ID']) == len(zonal_stats)
    # deleting unzipped
    rm_res = subprocess.call(['rm','-rf',unzip_path])
    data = [(polygon_index,
                       epa_l4_id,
                       epa_l3_id,
                       year,
                       stats['mean'],
                       stats['std'],
                       stats['count'])
        for polygon_index,(epa_l4_id, epa_l3_id, stats) in enumerate(zip(GDF['EPA_L4_ID'],GDF['EPA_L3_ID'],zonal_stats))]

    data_df = pd.DataFrame(data , columns = DATA_COLUMNS[:-1])
    if month is not None:
        data_df[DATA_COLUMNS[-1]] = month

    # mkdir -p
    os.makedirs(output_dir, exist_ok=True)

    # dumping data into parquet file
    data_df.to_parquet(output_path)


def parallel_process(epa_shapefile, output_basedir, entries, max_workers = 30):
    with ProcessPoolExecutor(max_workers= max_workers) as ex:
        tasks = [ex.submit(process_file,epa_shapefile, output_basedir, *args) for args in entries]
        with tqdm(total=len(tasks)) as pbar:
            for res in as_completed(tasks):
                res.result()
                pbar.update(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='Script to aggregate PRISM_LT spatially by polygons of EPA Ecoregions')
    parser.add_argument('epa_shapefile', type=Path)
    parser.add_argument('prism_basedir', type=Path, default = '.')
    parser.add_argument('--output-basedir', type=Path, default = './parquet_output')
    parser.add_argument('--max-workers', type=int, default=30)
    args = parser.parse_args()

    glob_pattern = f"{args.prism_basedir}/800m/*/monthly/*/*.zip"
    print(f"Aggregating all in {glob_pattern} by ecoregions in {args.epa_shapefile} to parquet in {args.output_basedir}")
    zipfile_pattern = re.compile(r'prism_([^_]+)_us_30s_(\d{4})(\d{2})?.zip$')
    get_filepath_var_year_month = lambda filepath: m.groups() if (m :=zipfile_pattern.search(filepath)) else None
    all_files = [(file,get_filepath_var_year_month(file)) for file in sorted(glob.glob(glob_pattern))]
    files = [(f,*pf) for f,pf in all_files if pf] # (filepath, var, year, month)
    assert len(files) == len(all_files),  [(f,*pf) for f,pf in all_files if pf is None] # (filepath, var, year, month)
    #filtered_files = (filepath for filepath in files if 
    monthly_entries = [(f,var, year, month) for f,var, year, month in files if month is not None] # (filepath, var, year, month)
    yearly_entries = [(f,var, year) for f,var, year, month in files if month is None] # (filepath, var, year)


    print("PROCESSING YEARLY")
    parallel_process(args.epa_shapefile, args.output_basedir, yearly_entries, max_workers = args.max_workers)

    print("PROCESSING MONTHLY")
    parallel_process(args.epa_shapefile, args.output_basedir, monthly_entries, max_workers = args.max_workers)
