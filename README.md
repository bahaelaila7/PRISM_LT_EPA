# PRISM_LT by EPA Ecoregions

Scripts to download and preprocess the PRISM longitudinal climate data, aggregating spatially by [EPA Ecoregions](https://www.epa.gov/eco-research/level-iii-and-iv-ecoregions-continental-united-states).

I have also committed the processed database `prism_lt_epa.db` through git-lfs (~1.9GB)

To get the database file, just clone this repo and do `$git lfs pull` 



## Usage

`$sh run.sh`

### WARNING: The PRISM_LT dataset is huge; As of this writing, it's ~559GB and spans from Jan 1895 to Feb 2025. As such I have commented the download command in `run.sh`. Uncomment the line if you need to download PRISM_LT data.



## Packages
These python packages are required:

```
pandas
pandasql
rasterio
rasterstats
geopandas
tqdm
```

Tested with python 3.13

## Output

An SQLite database `prism_lt_epa.db` with the following tables:

|Table|Description|
|-----|-----------|
|`monthly` |Spatial aggregation of the monthly rasters by all the polygons in the EPA shapefile|
|`yearly`| Same as `monthly` but using the yearly rasters |
|`EPA_L4_monthly`| Aggregating spatial statistics of the polygons in `monthly` by EPA Level 4|
|`EPA_L4_yearly`| same as `EPA_L4_monthy` but using `yearly`|
|`EPA_L3_monthly`| same as `EPA_L4_monthy` but aggregating by EPA Level 3|
|`EPA_L3_yearly`| same as `EPA_L4_yearly` but aggregating by EPA Level 3|

## Variables in PRISM_LT:

- `ppt`: Precipitation (in.)
- `tmin`: Minimum Temperature (°F)
- `tmean`: Average Temperature (°F)
- `tmax`: Maximum Temperature (°F)
- `tdmean`: Mean Dew Point Temperature (°F)
- `vpdmin`: Minimum Vapour Pressure Deficit (hPa)
- `vpdmax`: Max Vapour Pressure Deficit (hPa)

## Aggregated statistics:

In `monthly` and `yearly`, for each polygon a spatial mean and std of each variable is reported (eg, columns `ppt_mean` and `ppt_std`), together with the count of pixels in the polygon `PXL_COUNT`, the polygons EPA levels `EPA_L{3,4}_ID`, the year `Year` (and month `MONTH` if `monthly`).

In `EPA_L{3,4}_{monthy,yearly}`, `monthly` and `yearly` are groupged again by the EPA level, and the statistics reworked for the group.
