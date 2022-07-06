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

package org.apache.spark.sql.execution.command.v1

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.StringType

/**
 * This base suite contains unified tests for the `DESCRIBE TABLE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeTableSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.DescribeTableSuite`
 */
trait DescribeTableSuiteBase extends command.DescribeTableSuiteBase
  with command.TestsV1AndV2Commands {

  def getProvider(): String = defaultUsing.stripPrefix("USING").trim.toLowerCase(Locale.ROOT)

  test("Describing of a non-existent partition") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message === "Partition not found in table 'table' database 'ns':\nid -> 1")
    }
  }
}

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V1 In-Memory
 * table catalog.
 */
class DescribeTableSuite extends DescribeTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[DescribeTableSuiteBase].commandVersion

  test("DESCRIBE TABLE EXTENDED of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " TBLPROPERTIES ('bar'='baz')" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf.filter("col_name != 'Created Time'"),
        Seq(
          Row("data", "string", null),
          Row("id", "bigint", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Catalog", SESSION_CATALOG_NAME, ""),
          Row("Database", "ns", ""),
          Row("Table", "table", ""),
          Row("Last Access", "UNKNOWN", ""),
          Row("Created By", "Spark 3.4.0-SNAPSHOT", ""),
          Row("Type", "EXTERNAL", ""),
          Row("Provider", getProvider(), ""),
          Row("Comment", "this is a test table", ""),
          Row("Table Properties", "[bar=baz]", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Partition Provider", "Catalog", "")))
    }
  }
}
