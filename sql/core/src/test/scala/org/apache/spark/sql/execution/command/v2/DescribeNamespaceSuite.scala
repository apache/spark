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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StringType, StructType}
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `DESCRIBE NAMESPACE` command to check V2 table catalogs.
 */
class DescribeNamespaceSuite extends command.DescribeNamespaceSuiteBase with CommandSuiteBase {
  override def notFoundMsgPrefix: String = "Namespace"

  test("DescribeNamespace using v2 catalog") {
    withNamespace(s"$catalog.ns1.ns2") {
      sql(
        s"""
           | CREATE NAMESPACE IF NOT EXISTS $catalog.ns1.ns2
           | COMMENT 'test namespace'
           | LOCATION '/tmp/ns_test'
           | WITH DBPROPERTIES (password = 'password')
           """.stripMargin)
      val descriptionDf = sql(s"DESCRIBE NAMESPACE EXTENDED $catalog.ns1.ns2")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("info_name", StringType),
          ("info_value", StringType)
        ))
      val description = descriptionDf.collect()
      assert(description === Seq(
        Row("Catalog Name", catalog),
        Row("Namespace Name", "ns1.ns2"),
        Row(SupportsNamespaces.PROP_COMMENT.capitalize, "test namespace"),
        Row(SupportsNamespaces.PROP_LOCATION.capitalize, "file:/tmp/ns_test"),
        Row(SupportsNamespaces.PROP_OWNER.capitalize, Utils.getCurrentUserName()),
        Row("Properties", "((password,*********(redacted)))"))
      )
    }
  }

  test("SPARK-34577: drop/add columns to a dataset of `DESCRIBE NAMESPACE`") {
    withNamespace("ns") {
      sql("CREATE NAMESPACE ns")
      val description = sql(s"DESCRIBE NAMESPACE ns")
      val noCommentDataset = description.drop("info_name")
      val expectedSchema = new StructType()
        .add(
          name = "info_value",
          dataType = StringType,
          nullable = true,
          metadata = new MetadataBuilder()
            .putString("comment", "value of the namespace info").build())
      assert(noCommentDataset.schema === expectedSchema)
      val isNullDataset = noCommentDataset
        .withColumn("is_null", noCommentDataset("info_value").isNull)
      assert(isNullDataset.schema === expectedSchema.add("is_null", BooleanType, false))
    }
  }
}
