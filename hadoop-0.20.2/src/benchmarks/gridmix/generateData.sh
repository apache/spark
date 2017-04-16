#!/usr/bin/env bash

GRID_DIR=`dirname "$0"`
GRID_DIR=`cd "$GRID_DIR"; pwd`
source $GRID_DIR/gridmix-env

# Smaller data set is used by default.
COMPRESSED_DATA_BYTES=2147483648
UNCOMPRESSED_DATA_BYTES=536870912
INDIRECT_DATA_BYTES=58720256

# Number of partitions for output data
if [ -z ${NUM_MAPS} ] ; then
  NUM_MAPS=100
fi

INDIRECT_DATA_FILES=200

# If the env var USE_REAL_DATASET is set, then use the params to generate the bigger (real) dataset.
if [ ! -z ${USE_REAL_DATASET} ] ; then
  echo "Using real dataset"
  # 2TB data compressing to approx 500GB
  COMPRESSED_DATA_BYTES=2147483648000
  # 500GB
  UNCOMPRESSED_DATA_BYTES=536870912000
  # Default approx 70MB per data file, compressed
  INDIRECT_DATA_BYTES=58720256000 
fi

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${COMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${COMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=5 \
  -D test.randomtextwrite.max_words_key=10 \
  -D test.randomtextwrite.min_words_value=100 \
  -D test.randomtextwrite.max_words_value=10000 \
  -D mapred.output.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  ${VARCOMPSEQ} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${COMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${COMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=5 \
  -D test.randomtextwrite.max_words_key=5 \
  -D test.randomtextwrite.min_words_value=100 \
  -D test.randomtextwrite.max_words_value=100 \
  -D mapred.output.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  ${FIXCOMPSEQ} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${UNCOMPRESSED_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${UNCOMPRESSED_DATA_BYTES} / ${NUM_MAPS})) \
  -D test.randomtextwrite.min_words_key=1 \
  -D test.randomtextwrite.max_words_key=10 \
  -D test.randomtextwrite.min_words_value=0 \
  -D test.randomtextwrite.max_words_value=200 \
  -D mapred.output.compress=false \
  -outFormat org.apache.hadoop.mapred.TextOutputFormat \
  ${VARINFLTEXT} &

${HADOOP_HOME}/bin/hadoop jar \
  ${EXAMPLE_JAR} randomtextwriter \
  -D test.randomtextwrite.total_bytes=${INDIRECT_DATA_BYTES} \
  -D test.randomtextwrite.bytes_per_map=$((${INDIRECT_DATA_BYTES} / ${INDIRECT_DATA_FILES})) \
  -D test.randomtextwrite.min_words_key=5 \
  -D test.randomtextwrite.max_words_key=5 \
  -D test.randomtextwrite.min_words_value=20 \
  -D test.randomtextwrite.max_words_value=20 \
  -D mapred.output.compress=true \
  -D mapred.map.output.compression.type=BLOCK \
  -outFormat org.apache.hadoop.mapred.TextOutputFormat \
  ${FIXCOMPTEXT} &
