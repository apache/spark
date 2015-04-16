/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.test.TestHive

/**
 * Test the aggregation framework2.
 */
class Aggregate2CompatibilitySuite extends HiveCompatibilitySuite {
  override def beforeAll() {
    super.beforeAll()
    TestHive.setConf(SQLConf.AGGREGATE_2, "true")
  }

  override def afterAll() {
    TestHive.setConf(SQLConf.AGGREGATE_2, "false")
    super.afterAll()
  }

  override def whiteList = Seq(
    "groupby1",
    "groupby11",
    "groupby12",
    "groupby1_limit",
    "groupby_grouping_id1",
    "groupby_grouping_id2",
    "groupby_grouping_sets1",
    "groupby_grouping_sets2",
    "groupby_grouping_sets3",
    "groupby_grouping_sets4",
    "groupby_grouping_sets5",
    "groupby1_map",
    "groupby1_map_nomap",
    "groupby1_map_skew",
    "groupby1_noskew",
    "groupby2",
    "groupby2_limit",
    "groupby2_map",
    "groupby2_map_skew",
    "groupby2_noskew",
    "groupby4",
    "groupby4_map",
    "groupby4_map_skew",
    "groupby4_noskew",
    "groupby5",
    "groupby5_map",
    "groupby5_map_skew",
    "groupby5_noskew",
    "groupby6",
    "groupby6_map",
    "groupby6_map_skew",
    "groupby6_noskew",
    "groupby7",
    "groupby7_map",
    "groupby7_map_multi_single_reducer",
    "groupby7_map_skew",
    "groupby7_noskew",
    "groupby7_noskew_multi_single_reducer",
    "groupby8",
    "groupby8_map",
    "groupby8_map_skew",
    "groupby8_noskew",
    "groupby9",
    "groupby_distinct_samekey",
    "groupby_map_ppr",
    "groupby_multi_insert_common_distinct",
    "groupby_multi_single_reducer2",
    "groupby_multi_single_reducer3",
    "groupby_mutli_insert_common_distinct",
    "groupby_neg_float",
    "groupby_ppd",
    "groupby_ppr",
    "groupby_sort_10",
    "groupby_sort_2",
    "groupby_sort_3",
    "groupby_sort_4",
    "groupby_sort_5",
    "groupby_sort_6",
    "groupby_sort_7",
    "groupby_sort_8",
    "groupby_sort_9",
    "groupby_sort_test_1",
    "having",
    "udaf_collect_set",
    "udaf_corr",
    "udaf_covar_pop",
    "udaf_covar_samp",
    "udaf_histogram_numeric",
    "udaf_number_format",
    "udf_sum",
    "udf_avg",
    "udf_count"
  )
}
