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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, Join}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class BroadcastHintSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  test("broadcast hint on Hive table") {
    withTable("hive_t", "hive_u") {
      spark.sql("CREATE TABLE hive_t(a int)")
      spark.sql("CREATE TABLE hive_u(b int)")

      val hive_t = spark.table("hive_t").queryExecution.analyzed
      val hive_u = spark.table("hive_u").queryExecution.analyzed

      val plan = spark.sql("SELECT /*+ MAPJOIN(hive_t) */ * FROM hive_t, hive_u")
        .queryExecution.analyzed

      assert(plan.collectFirst {
        case BroadcastHint(MetastoreRelation(_, "hive_t")) => true
      }.isDefined)
      assert(plan.collectFirst {
        case Join(_, MetastoreRelation(_, "hive_u"), _, _) => true
      }.isDefined)

      val plan2 = spark.sql("SELECT /*+ MAPJOIN(hive_u) */ a FROM hive_t, hive_u")
        .queryExecution.analyzed

      assert(plan2.collectFirst {
        case BroadcastHint(MetastoreRelation(_, "hive_u")) => true
      }.isDefined)
      assert(plan2.collectFirst {
        case Join(MetastoreRelation(_, "hive_t"), _, _, _) => true
      }.isDefined)
    }
  }
}
