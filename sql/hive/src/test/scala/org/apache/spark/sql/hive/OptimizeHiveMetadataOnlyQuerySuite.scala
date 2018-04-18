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

package org.apache.spark.sql.hive

import org.scalatest.BeforeAndAfter

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class OptimizeHiveMetadataOnlyQuerySuite extends QueryTest with TestHiveSingleton
    with BeforeAndAfter with SQLTestUtils {

  test("SPARK-23877: validate metadata-only query pushes filters to metastore") {
    withTable("metadata_only") {
      sql("CREATE TABLE metadata_only (id bigint, data string) PARTITIONED BY (part int)")
      (0 to 100).foreach(p => sql(s"ALTER TABLE metadata_only ADD PARTITION (part=$p)"))

      // verify the number of matching partitions
      assert(sql("SELECT DISTINCT part FROM metadata_only WHERE part < 10").collect.size === 10)

      // verify that the partition predicate was pushed down to the metastore
      assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount === 10)
    }
  }
}
