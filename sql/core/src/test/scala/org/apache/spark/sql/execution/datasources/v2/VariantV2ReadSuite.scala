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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, VariantType}

class VariantV2ReadSuite extends QueryTest with SharedSparkSession {

  private val testCatalogClass = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  private def withV2Catalog(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.DEFAULT_CATALOG.key -> "testcat",
      s"spark.sql.catalog.testcat" -> testCatalogClass,
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "true",
      SQLConf.VARIANT_ALLOW_READING_SHREDDED.key -> "true") {
      f
    }
  }

  test("DSV2: push variant_get fields") {
    withV2Catalog {
      sql("DROP TABLE IF EXISTS testcat.ns.users")
      sql(
        """CREATE TABLE testcat.ns.users (
          |  id bigint,
          |  name string,
          |  v variant,
          |  vd variant default parse_json('1')
          |) USING parquet""".stripMargin)

      val out = sql(
        """
          |SELECT
          |  id,
          |  variant_get(v, '$.username', 'string') as username,
          |  variant_get(v, '$.age', 'int') as age
          |FROM testcat.ns.users
          |WHERE variant_get(v, '$.status', 'string') = 'active'
          |""".stripMargin)

      checkAnswer(out, Seq.empty)

      // Verify variant column rewrite
      val optimized = out.queryExecution.optimizedPlan
      val relOutput = optimized.collectFirst {
        case s: DataSourceV2ScanRelation => s.output
      }.getOrElse(fail("Expected DSv2 relation in optimized plan"))

      val vAttr = relOutput.find(_.name == "v").getOrElse(fail("Missing 'v' column"))
      vAttr.dataType match {
        case s: StructType =>
          assert(s.fields.length == 3,
            s"Expected 3 fields (username, age, status), got ${s.fields.length}")
          assert(s.fields.forall(_.metadata.contains(VariantMetadata.METADATA_KEY)),
            "All fields should have VariantMetadata")

          val paths = s.fields.map(f => VariantMetadata.fromMetadata(f.metadata).path).toSet
          assert(paths == Set("$.username", "$.age", "$.status"),
            s"Expected username, age, status paths, got: $paths")

          val fieldTypes = s.fields.map(_.dataType).toSet
          assert(fieldTypes.contains(StringType), "Expected StringType for string fields")
          assert(fieldTypes.contains(IntegerType), "Expected IntegerType for age")

        case other =>
          fail(s"Expected StructType for 'v', got: $other")
      }

      // Verify variant with default value is NOT rewritten
      relOutput.find(_.name == "vd").foreach { vdAttr =>
        assert(vdAttr.dataType == VariantType,
          "Variant column with default value should not be rewritten")
      }
    }
  }

  test("DSV2: nested column pruning for variant struct") {
    withV2Catalog {
      sql("DROP TABLE IF EXISTS testcat.ns.users2")
      sql(
        """CREATE TABLE testcat.ns.users2 (
          |  id bigint,
          |  name string,
          |  v variant
          |) USING parquet""".stripMargin)

      val out = sql(
        """
          |SELECT id, variant_get(v, '$.username', 'string') as username
          |FROM testcat.ns.users2
          |""".stripMargin)

      checkAnswer(out, Seq.empty)

      val scan = out.queryExecution.executedPlan.collectFirst {
        case b: BatchScanExec => b.scan
      }.getOrElse(fail("Expected BatchScanExec in physical plan"))

      val readSchema = scan.readSchema()

      // Verify 'v' field exists and is a struct
      val vField = readSchema.fields.find(_.name == "v").getOrElse(
        fail("Expected 'v' field in read schema")
      )

      vField.dataType match {
        case s: StructType =>
          assert(s.fields.length == 1,
            "Expected only 1 field ($.username) in pruned schema, got " + s.fields.length + ": " +
              s.fields.map(f => VariantMetadata.fromMetadata(f.metadata).path).mkString(", "))

          val field = s.fields(0)
          assert(field.metadata.contains(VariantMetadata.METADATA_KEY),
            "Field should have VariantMetadata")

          val metadata = VariantMetadata.fromMetadata(field.metadata)
          assert(metadata.path == "$.username",
            "Expected path '$.username', got '" + metadata.path + "'")
          assert(field.dataType == StringType,
            s"Expected StringType, got ${field.dataType}")

        case other =>
          fail(s"Expected StructType for 'v' after rewrite and pruning, got: $other")
      }
    }
  }
}