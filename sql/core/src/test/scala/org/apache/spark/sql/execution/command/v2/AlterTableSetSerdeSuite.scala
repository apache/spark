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
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. SET [SERDE|SERDEPROPERTIES]` command to
 * check V2 table catalogs.
 */
class AlterTableSetSerdeSuite extends command.AlterTableSetSerdeSuiteBase with CommandSuiteBase {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)

  test("v2 catalog doesn't support ALTER TABLE SerDe properties") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) " +
        s"USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')")
      }
      assert(e.message.contains(
        "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES] is not supported for v2 tables"))
    }
  }
}
