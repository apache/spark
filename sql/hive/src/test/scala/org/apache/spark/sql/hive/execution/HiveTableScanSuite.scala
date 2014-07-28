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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.test.TestHive

class HiveTableScanSuite extends HiveComparisonTest {
  // MINOR HACK: You must run a query before calling reset the first time.
  TestHive.hql("SHOW TABLES")
  TestHive.reset()

  TestHive.hql("""CREATE TABLE part_scan_test (key STRING, value STRING) PARTITIONED BY (ds STRING) 
                 | ROW FORMAT SERDE 
                 | 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' 
                 | STORED AS RCFILE
               """.stripMargin)
  TestHive.hql("""FROM src
                 | INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-01')
                 | SELECT 100,100 LIMIT 1
               """.stripMargin)
  TestHive.hql("""ALTER TABLE part_scan_test SET SERDE
                 | 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
               """.stripMargin)
  TestHive.hql("""FROM src INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-02')
                 | SELECT 200,200 LIMIT 1
               """.stripMargin)

  createQueryTest("partition_based_table_scan_with_different_serde", 
    "SELECT * from part_scan_test", false)
}
