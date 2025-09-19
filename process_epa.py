import argparse
import geopandas as gpd
from pathlib import Path


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='EPA Shapefile Processor')
    parser.add_argument('shapefile', type=Path)
    parser.add_argument('--output-shapefile', type=Path, default=None)

    args = parser.parse_args()

    output_path = args.output_shapefile or Path(args.shapefile.parent, 'l4_vector.gpkg')

    gdf = gpd.read_file(args.shapefile)
    gdf['EPA_L4_ID'] = gdf['NA_L3CODE'] +'.'+ gdf['US_L4CODE']
    gdf['EPA_L3_ID'] = gdf['NA_L3CODE']
    print(f"Saving to {output_path}")
    gdf.to_file(output_path)

