#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from sparktestsupport.shellutils import run_cmd

benchmarks = [
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.AggregateBenchmark'],
    ['avro/test', 'org.apache.spark.sql.execution.benchmark.AvroReadBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.BloomFilterBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.DataSourceReadBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.DateTimeBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.ExtractBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.FilterPushdownBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.InExpressionBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.IntervalBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.JoinBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.MakeDateTimeBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.MiscBenchmark'],
    ['hive/test', 'org.apache.spark.sql.execution.benchmark.ObjectHashAggregateExecBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.OrcNestedSchemaPruningBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.OrcV2NestedSchemaPruningBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.ParquetNestedSchemaPruningBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.RangeBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.UDFBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.WideSchemaBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.benchmark.WideTableBenchmark'],
    ['hive/test', 'org.apache.spark.sql.hive.orc.OrcReadBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.datasources.csv.CSVBenchmark'],
    ['sql/test', 'org.apache.spark.sql.execution.datasources.json.JsonBenchmark']
]

print('Set SPARK_GENERATE_BENCHMARK_FILES=1')
os.environ['SPARK_GENERATE_BENCHMARK_FILES'] = '1'

for b in benchmarks:
    print("Run benchmark: %s" % b[1])
    run_cmd(['build/sbt', '%s:runMain %s' % (b[0], b[1])])
