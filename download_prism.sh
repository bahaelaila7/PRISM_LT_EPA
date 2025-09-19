#!/usr/bin/env bash
export PRISM_BASEDIR="${PRISM_BASEDIR:=.}"
echo "Downloading PRISM DATA to $PRISM_BASEDIR"
lftp  prism.oregonstate.edu <<EOF
mirror --continue --verbose --parallel=8 /time_series/us/lt "${PRISM_BASEDIR}"
bye
EOF
