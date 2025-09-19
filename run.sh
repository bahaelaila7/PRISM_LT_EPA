#!/usr/bin/env bash

#set -x
set -e

export ECOREGIONS_DIR="./EPA_ECOREGIONS"
export EPA_OUTPUT="${ECOREGIONS_DIR}/l4_vector.gpkg"
export PRISM_BASEDIR="./lt_data"
export PARQUET_BASEDIR="./parquet_output"
export OUTPUT_DB="./prism_lt_epa.db"
export NUM_WORKERS=30

#sh download_epa.sh
#sh download_prism.sh
python process_prism.py "${EPA_OUTPUT}" "${PRISM_BASEDIR}" --output-basedir "${PARQUET_BASEDIR}" --max-workers $NUM_WORKERS 
python coalesce_by_agg.py "${PARQUET_BASEDIR}" "${OUTPUT_DB}"
python aggregate_polygons.py "${OUTPUT_DB}"
#sqlite3 "${OUTPUT_DB}" vacuum
python vacuum.py "${OUTPUT_DB}"
