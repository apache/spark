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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}

/**
 * This base suite contains unified tests for the `DESCRIBE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.DescribeTableSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.DescribeTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeTableSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.DescribeTableSuite`
 */
trait DescribeTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "DESCRIBE TABLE"

  test("DESCRIBE TABLE in a catalog when table does not exist") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE ${tbl}_non_existence")
      }
      assert(e.getMessage.contains(s"Table or view not found: ${tbl}_non_existence"))
    }
  }

  test("SPARK-34561: drop/add columns to a dataset of `DESCRIBE TABLE`") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (c0 INT) $defaultUsing")
      val description = sql(s"DESCRIBE TABLE $tbl")
      val noCommentDataset = description.drop("comment")
      val expectedSchema = new StructType()
        .add(
          name = "col_name",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "name of the column").build())
        .add(
          name = "data_type",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "data type of the column").build())
      assert(noCommentDataset.schema === expectedSchema)
      val isNullDataset = noCommentDataset
        .withColumn("is_null", noCommentDataset("col_name").isNull)
      assert(isNullDataset.schema === expectedSchema.add("is_null", BooleanType, false))
    }
  }

  test("SPARK-34576: drop/add columns to a dataset of `DESCRIBE COLUMN`") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (c0 INT) $defaultUsing")
      val description = sql(s"DESCRIBE TABLE $tbl c0")
      val noCommentDataset = description.drop("info_value")
      val expectedSchema = new StructType()
        .add(
          name = "info_name",
          dataType = StringType,
          nullable = false,
          metadata = new MetadataBuilder().putString("comment", "name of the column info").build())
      assert(noCommentDataset.schema === expectedSchema)
      val isNullDataset = noCommentDataset
        .withColumn("is_null", noCommentDataset("info_name").isNull)
      assert(isNullDataset.schema === expectedSchema.add("is_null", BooleanType, false))
    }
  }
}
