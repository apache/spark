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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.{SparkConf, SparkUnsupportedOperationException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.util.Utils

class DerbyTableCatalogSuite extends QueryTest with SharedSparkSession {

  val tempDir = Utils.createTempDir()
  val url = s"jdbc:derby:memory:${tempDir.getCanonicalPath};create=true"
  val defaultMetadata = new MetadataBuilder().putLong("scale", 0).build()

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.derby", classOf[JDBCTableCatalog].getName)
      .set("spark.sql.catalog.derby.url", url)
      .set("spark.sql.catalog.derby.driver", "org.apache.derby.jdbc.EmbeddedDriver")
  }

  test("SPARK-42978: RENAME cannot qualify a new-table-Name with a schema-Name.") {
    val n1t1 = "derby.test1.table1"
    val n1t2 = "test1.table2"
    val n2t2 = "test2.table2"

    withTable(n1t1, n1t2) {
      sql(s"CREATE TABLE $n1t1(c1 int)")
      checkError(
        exception = intercept[SparkUnsupportedOperationException](
          sql(s"ALTER TABLE $n1t1 RENAME TO $n2t2")),
        errorClass = "CANNOT_RENAME_ACROSS_SCHEMA",
        parameters = Map("type" -> "table"))
      sql(s"ALTER TABLE $n1t1 RENAME TO $n1t2")
      checkAnswer(sql(s"SHOW TABLES IN derby.test1"), Row("test1", "TABLE2", false))
    }
  }

  test("SPARK-48439: Calculate suitable precision and scale for DECIMAL type") {
    withTable("derby.test1.table1") {
      sql("CREATE TABLE derby.test1.table1 (c1 decimal(38, 18))")
      sql("INSERT INTO derby.test1.table1 VALUES (1.123456789123456789)")
      checkAnswer(sql("SELECT * FROM derby.test1.table1"), Row(1.12345678912))
    }
  }
}
