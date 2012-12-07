#!/bin/bash
set -o errexit -o nounset -o pipefail

table="$1"; shift

shuttercat --csv --above "COPY $table FROM STDIN (FORMAT CSV);" \
                 --below '\.' | psql "$@"

