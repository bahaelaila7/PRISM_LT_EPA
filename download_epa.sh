#!/usr/bin/env bash

ECOREGIONS_DIR="${ECOREGIONS_DIR:=./EPA_ECOREGIONS}"
EPA_OUTPUT="${EPA_OUTPUT:=${ECOREGIONS_DIR}/l4_vector.gpkg}"
echo "Downloading and extracting EPA Ecoregions to ${ECOREGIONS_DIR}"
set -x
set -e

curl -O 'https://dmap-prod-oms-edc.s3.us-east-1.amazonaws.com/ORD/Ecoregions/us/us_eco_l4.zip'
unzip -oqqd "${ECOREGIONS_DIR}" ./us_eco_l4.zip
python process_epa.py "${ECOREGIONS_DIR}" --output-shapefile "${EPA_OUTPUT}"
