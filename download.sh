#!/usr/bin/env bash

lftp  prism.oregonstate.edu <<EOF
mirror --continue --verbose --parallel=8 /time_series/us/lt .
bye
EOF
