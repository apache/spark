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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V2 table catalogs.
 */
class DescribeTableSuite extends command.DescribeTableSuiteBase with CommandSuiteBase {
  override def namespace: String = "ns1.ns2"

  test("DESCRIBE TABLE using v2 catalog") {
    withNamespaceAndTable(namespace, "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("col_name", StringType),
          ("data_type", StringType),
          ("comment", StringType)))
      val description = descriptionDf.collect()
      assert(description === Seq(
        Row("data", "string", ""),
        Row("id", "bigint", ""),
        Row("", "", ""),
        Row("# Partitioning", "", ""),
        Row("Part 0", "id", "")))

      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message.contains("DESCRIBE does not support partition for v2 tables"))
    }
  }

  test("DESCRIBE TABLE with v2 catalog when table does not exist.") {
    intercept[AnalysisException] {
      spark.sql(s"DESCRIBE TABLE $catalog.$namespace.non_existence")
    }
  }

  test("DESCRIBE TABLE EXTENDED using v2 catalog") {
    val tbl = s"$catalog.$namespace.tbl"
    withTable(tbl) {
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " TBLPROPERTIES ('bar'='baz')" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType))
        === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      assert(descriptionDf.collect()
        .map(_.toSeq)
        .map(_.toArray.map(_.toString.trim)) === Array(
        Array("data", "string", ""),
        Array("id", "bigint", ""),
        Array("", "", ""),
        Array("# Partitioning", "", ""),
        Array("Part 0", "id", ""),
        Array("", "", ""),
        Array("# Metadata Columns", "", ""),
        Array("index", "int", "Metadata column used to conflict with a data column"),
        Array("_partition", "string", "Partition key used to store the row"),
        Array("", "", ""),
        Array("# Detailed Table Information", "", ""),
        Array("Name", tbl, ""),
        Array("Comment", "this is a test table", ""),
        Array("Location", "file:/tmp/testcat/table_name", ""),
        Array("Provider", defaultProvider, ""),
        Array(TableCatalog.PROP_OWNER.capitalize, Utils.getCurrentUserName(), ""),
        Array("Table Properties", "[bar=baz]", "")))
    }
  }

  test("SPARK-34561: drop/add columns to a dataset of `DESCRIBE TABLE`") {
    val tbl = s"$catalog.$namespace.tbl"
    withTable(tbl) {
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
}
