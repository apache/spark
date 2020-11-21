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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.test.SharedSparkSession

class ShowPartitionsSuite extends command.ShowPartitionsSuiteBase with SharedSparkSession {
  override def version: String = "V2"
  override def catalog: String = "test_catalog"
  override def defaultNamespace: Seq[String] = Nil
  override def defaultUsing: String = "USING _"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryTableCatalog].getName)

  // TODO(SPARK-33452): Create a V2 SHOW PARTITIONS execution node
  test("not supported SHOW PARTITIONS") {
    def testV1Command(sqlCommand: String, sqlParams: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(s"$sqlCommand $sqlParams")
      }
      assert(e.message.contains(s"$sqlCommand is only supported with v1 tables"))
    }
    val t = s"$catalog.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |$defaultUsing
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("SHOW PARTITIONS", t)
      testV1Command("SHOW PARTITIONS", s"$t PARTITION(id='1')")
    }
  }
}
