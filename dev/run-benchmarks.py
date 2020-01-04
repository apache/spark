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
    'org.apache.spark.sql.execution.benchmark.AggregateBenchmark',
    'org.apache.spark.sql.execution.benchmark.AvroReadBenchmark',
    'org.apache.spark.sql.execution.benchmark.BloomFilterBenchmark',
    'org.apache.spark.sql.execution.benchmark.DataSourceReadBenchmark',
    'org.apache.spark.sql.execution.benchmark.DateTimeBenchmark',
    'org.apache.spark.sql.execution.benchmark.ExtractBenchmark',
    'org.apache.spark.sql.execution.benchmark.FilterPushdownBenchmark',
    'org.apache.spark.sql.execution.benchmark.InExpressionBenchmark',
    'org.apache.spark.sql.execution.benchmark.IntervalBenchmark',
    'org.apache.spark.sql.execution.benchmark.JoinBenchmark',
    'org.apache.spark.sql.execution.benchmark.MakeDateTimeBenchmark',
    'org.apache.spark.sql.execution.benchmark.MiscBenchmark',
    'org.apache.spark.sql.execution.benchmark.NestedSchemaPruningBenchmark',
    'org.apache.spark.sql.execution.benchmark.ObjectHashAggregateExecBenchmark',
    'org.apache.spark.sql.execution.benchmark.RangeBenchmark',
    'org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark',
    'org.apache.spark.sql.execution.benchmark.UDFBenchmark',
    'org.apache.spark.sql.execution.benchmark.WideSchemaBenchmark',
    'org.apache.spark.sql.execution.benchmark.WideTableBenchmark',
    'org.apache.spark.sql.hive.orc.OrcReadBenchmark',
    'org.apache.spark.sql.execution.datasources.csv.CSVBenchmark',
    'org.apache.spark.sql.execution.datasources.json.JsonBenchmark'
]

print('Set SPARK_GENERATE_BENCHMARK_FILES=1')
os.environ['SPARK_GENERATE_BENCHMARK_FILES'] = '1'

for benchmark in benchmarks:
    print("Run benchmark: %s" % benchmark)
    run_cmd(['build/sbt', 'sql/test:runMain %s' % benchmark])
