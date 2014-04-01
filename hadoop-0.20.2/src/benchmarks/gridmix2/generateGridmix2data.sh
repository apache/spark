#!/usr/bin/env bash
 
##############################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#####################################################################

GRID_DIR=`dirname "$0"`
GRID_DIR=`cd "$GRID_DIR"; pwd`
source $GRID_DIR/gridmix-env-2

# Smaller data set is used by default.
COMPRESSED_DATA_BYTES=2147483648
UNCOMPRESSED_DATA_BYTES=536870912

# Number of partitions for output data
NUM_MAPS=100

# If the env var USE_REAL_DATASET is set, then use the params to generate the bigger (real) dataset.
if [ ! -z ${USE_REAL_DATASET} ] ; then
  echo "Using real dataset"
  NUM_MAPS=492
  # 2TB data compressing to approx 500GB
  COMPRESSED_DATA_BYTES=2147483648000
  # 500GB
  UNCOMPRESSED_DATA_BYTES=536870912000
fi

## Data sources
export GRID_MIX_DATA=/gridmix/data
# Variable length key, value compressed SequenceFile
export VARCOMPSEQ=${GRID_MIX_DATA}/WebSimulationBlockCompressed
# Fixed length key, value compressed SequenceFile
export FIXCOMPSEQ=${GRID_MIX_DATA}/MonsterQueryBlockCompressed
# Variable length key, value uncompressed Text File
export VARINFLTEXT=${GRID_MIX_DATA}/SortUncompressed
# Fixed length key, value compressed Text File
export FIXCOMPTEXT=${GRID_MIX_DATA}/EntropySimulationCompressed

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


