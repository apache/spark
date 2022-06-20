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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.StringType

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V2 table catalogs.
 */
class DescribeTableSuite extends command.DescribeTableSuiteBase with CommandSuiteBase {

  test("DESCRIBE TABLE with non-'partitioned-by' clause") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("col_name", StringType),
          ("data_type", StringType),
          ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("data", "string", ""),
          Row("id", "bigint", ""),
          Row("", "", ""),
          Row("# Partitioning", "", ""),
          Row("Not partitioned", "", "")))
    }
  }

  test("Describing a partition is not supported") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message === "DESCRIBE does not support partition for v2 tables.")
    }
  }
}
