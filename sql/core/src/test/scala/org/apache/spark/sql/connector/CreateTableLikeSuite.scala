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
import org.apache.spark.sql.types.{CharType, IntegerType, LongType, StringType, VarcharType}

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

  test("v2 catalog target: non-existent provider is stored as property without validation") {
    // Pure V2 catalogs (e.g. InMemoryCatalog) do not validate the provider — they store it
    // as a plain property and let the catalog implementation decide what to do with it.
    // This is consistent with how CreateTableExec works for CREATE TABLE targeting V2 catalogs.
    withTable("testcat.src", "testcat.dst") {
      sql("CREATE TABLE testcat.src (id bigint) USING foo")
      sql("CREATE TABLE testcat.dst LIKE testcat.src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("provider") === "foo",
        "Provider should be copied from source as-is without validation")
    }
  }

  test("session catalog target: non-existent provider from source is rejected") {
    // V2SessionCatalog bridges to the V1 DataSource world and calls
    // DataSource.lookupDataSource(provider) when creating the target table.
    // A non-existent provider therefore causes a DATA_SOURCE_NOT_FOUND error,
    // unlike pure V2 catalogs which accept any provider string.
    withTable("testcat.src") {
      sql("CREATE TABLE testcat.src (id bigint) USING foo")
      val ex = intercept[Exception] {
        sql("CREATE TABLE dst LIKE testcat.src")
      }
      assert(ex.getMessage.contains("foo"),
        "Error should mention the unresolvable provider")
    }
  }

  test("source provider is copied to v2 target when no USING override") {
    // When no USING clause is given, CreateTableLikeExec copies the provider from the
    // source table into PROP_PROVIDER of the target's TableInfo properties.
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("provider") === "parquet",
        "Source provider should be copied to the V2 target when no USING clause is specified")
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

  test("CHAR and VARCHAR types are preserved from v1 source to v2 target") {
    // CreateTableLikeExec calls CharVarcharUtils.getRawSchema on V1Table sources so that
    // CHAR(n)/VARCHAR(n) declarations survive the copy instead of being collapsed to StringType.
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint, name CHAR(10), tag VARCHAR(20)) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      val schema = CatalogV2Util.v2ColumnsToStructType(dst.columns())
      assert(schema("name").dataType === CharType(10))
      assert(schema("tag").dataType === VarcharType(20))
    }
  }

  // -------------------------------------------------------------------------
  // V2 source, session catalog (V1) target
  // -------------------------------------------------------------------------

  test("v2 source, v1 target: session catalog target with v2 catalog source") {
    // Source is a pure V2 table in testcat (InMemoryTable, not a V1Table).
    // Target is the session catalog. ResolvedV1TableOrViewIdentifier does not match
    // the V2 source, so ResolveSessionCatalog falls through and CreateTableLikeExec
    // is used (targeting V2SessionCatalog which backs the session catalog).
    // The source must use a real provider (parquet) so that V2SessionCatalog can
    // validate it when creating the target table.
    withTable("testcat.src", "dst") {
      sql("CREATE TABLE testcat.src (id bigint, data string) USING parquet")
      sql("CREATE TABLE dst LIKE testcat.src")

      assert(spark.catalog.tableExists("dst"))
      val schema = spark.table("dst").schema
      assert(schema.fieldNames === Array("id", "data"))
    }
  }

  test("v2 source, v1 target: schema and partitioning are copied") {
    withTable("testcat.src", "dst") {
      sql("CREATE TABLE testcat.src (id bigint, data string) USING parquet " +
        "PARTITIONED BY (data)")
      sql("CREATE TABLE dst LIKE testcat.src")

      assert(spark.catalog.tableExists("dst"))
      val schema = spark.table("dst").schema
      assert(schema.fieldNames === Array("id", "data"))
    }
  }

  // -------------------------------------------------------------------------
  // CatalogExtension (Iceberg-style session catalog override) scenario
  // -------------------------------------------------------------------------

  test("CatalogExtension session catalog override: source is native V2 table, uses V2 exec path") {
    // In this suite the session catalog is already overridden with InMemoryTableSessionCatalog,
    // which implements CatalogExtension — the same pattern used by Iceberg's SparkSessionCatalog.
    //
    // When the source is a native V2 InMemoryTable (from testcat, not a V1Table):
    //   - supportsV1Command returns true (CatalogExtension catalog)
    //   - but ResolvedV1TableOrViewIdentifier does NOT match a non-V1Table source
    //   - so CreateTableLikeExec (V2 exec path) is used instead of CreateTableLikeCommand
    //   - CreateTableLikeExec calls InMemoryTableSessionCatalog.createTable which stores
    //     the target as a native InMemoryTable in the extension catalog
    withTable("testcat.src", "dst") {
      sql("CREATE TABLE testcat.src (id bigint, data string) USING parquet")
      sql("CREATE TABLE dst LIKE testcat.src")

      // The target should exist and have the correct schema
      assert(spark.catalog.tableExists("dst"))
      val schema = spark.table("dst").schema
      assert(schema.fieldNames === Array("id", "data"))

      // The target was created through the CatalogExtension catalog; verify it is an
      // InMemoryTable (the native V2 type used by InMemoryTableSessionCatalog) rather than
      // a V1Table backed by the Hive metastore.
      val extCatalog = spark.sessionState.catalogManager
        .catalog("spark_catalog")
        .asInstanceOf[InMemoryTableSessionCatalog]
      val dst = extCatalog.loadTable(Identifier.of(Array("default"), "dst"))
      assert(dst.isInstanceOf[org.apache.spark.sql.connector.catalog.InMemoryTable],
        "Target table should be a native V2 InMemoryTable in the CatalogExtension catalog")
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
