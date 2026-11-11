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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.v2.ffi.NativeUDFTestUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton

/**
 * Tests native functions on Hive tables, with a package whose library Spark never loads: the
 * tests only plan queries. NativeUDFSuite tests the rest.
 */
class NativeUDFHiveSuite extends QueryTest with TestHiveSingleton {

  test("prune the partitions of a Hive table without the filters that call native functions") {
    withTable("native_partitioned") {
      sql("CREATE TABLE native_partitioned (id BIGINT) PARTITIONED BY (p BIGINT) " +
        "STORED AS TEXTFILE")
      (0 until 3).foreach { p =>
        sql(s"INSERT INTO native_partitioned PARTITION (p = $p) VALUES ($p)")
      }
      val session = spark.newSession()
      NativeUDFTestUtils.registerForPlans(session)
      val plan = session.sql("SELECT id FROM native_partitioned WHERE p < 2 AND add_one(p) = 2")
        .queryExecution.optimizedPlan
      val pruned = plan.collect { case relation: HiveTableRelation => relation.prunedPartitions }
      assert(pruned.map(_.map(_.map(_.spec("p")).sorted)) == Seq(Some(Seq("0", "1"))), plan)
    }
  }
}
