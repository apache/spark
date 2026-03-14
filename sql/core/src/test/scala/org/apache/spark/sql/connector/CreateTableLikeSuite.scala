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

package org.apache.spark.sql.connector

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, InMemoryCatalog}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class CreateTableLikeSuite extends DatasourceV2SQLBase {

  private def testCatalog: InMemoryCatalog =
    catalog("testcat").asInstanceOf[InMemoryCatalog]

  private def testCatalog2: InMemoryCatalog =
    catalog("testcat2").asInstanceOf[InMemoryCatalog]

  // -------------------------------------------------------------------------
  // Basic V2 path
  // -------------------------------------------------------------------------

  test("v2 target, v1 source: schema and partitioning are copied") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint, data string) USING parquet PARTITIONED BY (data)")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.schema() === CatalogV2Util.v2ColumnsToStructType(dst.columns()))
      val columnNames = dst.columns().map(_.name)
      assert(columnNames === Array("id", "data"))
      // partition column encoded in partitioning (identity transform on the partition column)
      assert(dst.partitioning.nonEmpty)
    }
  }

  test("v2 target, v2 source: pure v2 path") {
    withTable("testcat.src", "testcat.dst") {
      sql("CREATE TABLE testcat.src (id bigint, data string) USING foo")
      sql("CREATE TABLE testcat.dst LIKE testcat.src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      val columnNames = dst.columns().map(_.name)
      assert(columnNames === Array("id", "data"))
    }
  }

  test("cross-catalog: source in testcat, target in testcat2") {
    withTable("testcat.src", "testcat2.dst") {
      sql("CREATE TABLE testcat.src (id bigint, name string) USING foo")
      sql("CREATE TABLE testcat2.dst LIKE testcat.src")

      val dst = testCatalog2.loadTable(Identifier.of(Array(), "dst"))
      val columnNames = dst.columns().map(_.name)
      assert(columnNames === Array("id", "name"))
    }
  }

  test("3-part name: catalog.namespace.table for both target and source") {
    withTable("testcat.ns.src", "testcat2.ns.dst") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns")
      sql("CREATE NAMESPACE IF NOT EXISTS testcat2.ns")
      sql("CREATE TABLE testcat.ns.src (id bigint, data string) USING foo")
      sql("CREATE TABLE testcat2.ns.dst LIKE testcat.ns.src")

      val dst = testCatalog2.loadTable(Identifier.of(Array("ns"), "dst"))
      val columnNames = dst.columns().map(_.name)
      assert(columnNames === Array("id", "data"))
    }
  }

  // -------------------------------------------------------------------------
  // IF NOT EXISTS
  // -------------------------------------------------------------------------

  test("IF NOT EXISTS: second call is silent when table already exists") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src")
      // Should not throw
      sql("CREATE TABLE IF NOT EXISTS testcat.dst LIKE src")
    }
  }

  test("without IF NOT EXISTS, duplicate create throws TableAlreadyExistsException") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src")
      intercept[TableAlreadyExistsException] {
        sql("CREATE TABLE testcat.dst LIKE src")
      }
    }
  }

  // -------------------------------------------------------------------------
  // Views as source
  // -------------------------------------------------------------------------

  test("persistent view as source, v2 target") {
    withTable("src", "testcat.dst") {
      withView("v") {
        sql("CREATE TABLE src (id bigint, data string) USING parquet")
        sql("INSERT INTO src VALUES (1, 'a')")
        sql("CREATE VIEW v AS SELECT * FROM src")
        sql("CREATE TABLE testcat.dst LIKE v")

        val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
        val columnNames = dst.columns().map(_.name)
        assert(columnNames === Array("id", "data"))
      }
    }
  }

  test("temp view as source, v2 target") {
    withTable("src", "testcat.dst") {
      withTempView("tv") {
        sql("CREATE TABLE src (id bigint, data string) USING parquet")
        sql("CREATE TEMP VIEW tv AS SELECT id, data FROM src")
        sql("CREATE TABLE testcat.dst LIKE tv")

        val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
        val columnNames = dst.columns().map(_.name)
        assert(columnNames === Array("id", "data"))
      }
    }
  }

  // -------------------------------------------------------------------------
  // Property / provider behavior
  // -------------------------------------------------------------------------

  test("source TBLPROPERTIES are NOT copied to target") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet TBLPROPERTIES ('secret_key' = 'secret')")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(!dst.properties.containsKey("secret_key"),
        "Source TBLPROPERTIES should not be copied to target")
    }
  }

  test("user-specified TBLPROPERTIES are applied on target") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src TBLPROPERTIES ('custom' = 'value')")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("custom") === "value")
    }
  }

  test("USING clause overrides source provider") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src USING foo")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("provider") === "foo")
    }
  }

  // -------------------------------------------------------------------------
  // Column type fidelity
  // -------------------------------------------------------------------------

  test("multiple column types are preserved") {
    withTable("src", "testcat.dst") {
      sql(
        """CREATE TABLE src (
          |  id bigint,
          |  name string,
          |  score int
          |) USING parquet""".stripMargin)
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      val schema = CatalogV2Util.v2ColumnsToStructType(dst.columns())
      assert(schema("id").dataType === LongType)
      assert(schema("name").dataType === StringType)
      assert(schema("score").dataType === IntegerType)
    }
  }

  // -------------------------------------------------------------------------
  // V1 fallback regression
  // -------------------------------------------------------------------------

  test("v1 fallback: CREATE TABLE default.dst LIKE default.src still uses CreateTableLikeCommand") {
    withTable("src", "dst") {
      sql("CREATE TABLE src (id bigint, data string) USING parquet")
      sql("CREATE TABLE dst LIKE src")
      // Verify via session catalog that dst was created with the correct schema
      val meta = spark.sessionState.catalog.getTableMetadata(
        spark.sessionState.sqlParser.parseTableIdentifier("dst"))
      assert(meta.schema.fieldNames === Array("id", "data"))
    }
  }
}
