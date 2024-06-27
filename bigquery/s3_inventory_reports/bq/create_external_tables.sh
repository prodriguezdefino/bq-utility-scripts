#!/bin/bash
set -eux

if [ "$#" -ne 7 ]
  then
    echo "Usage : sh create_external_tables.sh <project> <source format> <bucket name> <bucket prefix path> <data files path> <partitioned files path> <bigquery dataset.table>"
    exit -1
fi

PROJECT=$1
BUCKET_NAME=$3
BUCKET_PREFIX_PATH=$4
BUCKET_PREFIX="gs://${BUCKET_NAME}/${BUCKET_PREFIX_PATH}"
SOURCE_FORMAT=$2
DATA_BUCKET_PATH="${BUCKET_PREFIX}/$5"
PARTITION_BUCKET_PATH="${BUCKET_PREFIX}/${6}/*/symlink.txt"
PARTITION_BUCKET_PREFIX="${BUCKET_PREFIX}/${6}"
DATA_DATASET_TABLE=$7
PARTITION_DATASET_TABLE="${DATA_DATASET_TABLE}_partition"
TABLE_FN_FQN="${DATA_DATASET_TABLE}_fn"

# create the data table
bq mkdef --source_format=$SOURCE_FORMAT --autodetect=true \
  $DATA_BUCKET_PATH > DATA_DEFINITION_FILE

bq mk --table \
  --external_table_definition=DATA_DEFINITION_FILE \
  $PROJECT:$DATA_DATASET_TABLE

# create the partition table
bq mkdef --source_format=CSV \
    --hive_partitioning_mode=AUTO \
    --hive_partitioning_source_uri_prefix=$PARTITION_BUCKET_PREFIX \
    --require_hive_partition_filter=true \
  $PARTITION_BUCKET_PATH > PARTITION_DEFINITION_FILE

bq mk --table \
  --external_table_definition=PARTITION_DEFINITION_FILE \
  $PROJECT:$PARTITION_DATASET_TABLE \
  data_file_s3_path:STRING

# create a table function to simplify access
QUERY=$(cat <<- EOQ
CREATE OR REPLACE TABLE FUNCTION $TABLE_FN_FQN (partition_prefix STRING)
AS (
    SELECT data.*, data._FILE_NAME as data_file_gcs_path, part.* FROM
    \`$PROJECT.$DATA_DATASET_TABLE\` as data JOIN
    \`$PROJECT.$PARTITION_DATASET_TABLE\` as part
    ON data._FILE_NAME = CONCAT('$BUCKET_PREFIX/', ARRAY_REVERSE(SPLIT(part.data_file_s3_path, '$BUCKET_PREFIX_PATH/'))[OFFSET(0)])
    WHERE part.dt LIKE(CONCAT(partition_prefix, '%'))
);
EOQ
)

bq query --use_legacy_sql=false $QUERY