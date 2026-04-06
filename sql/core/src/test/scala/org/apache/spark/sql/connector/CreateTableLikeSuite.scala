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
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, InMemoryCatalog, TableCatalog}
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

  test("source TBLPROPERTIES are copied to target when connector implements createTableLike") {
    // InMemoryTableCatalog overrides createTableLike to merge source properties into the target,
    // demonstrating connector-specific copy semantics. Connectors that do not override
    // createTableLike will throw UnsupportedOperationException.
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet TBLPROPERTIES ('source_key' = 'source')")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.containsKey("source_key"),
        "Connector-implemented createTableLike copies source TBLPROPERTIES to target")
    }
  }

  test("user-specified TBLPROPERTIES override source TBLPROPERTIES in createTableLike") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet TBLPROPERTIES ('key' = 'source_value')")
      sql("CREATE TABLE testcat.dst LIKE src TBLPROPERTIES ('key' = 'user_value')")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("key") == "user_value",
        "User-specified TBLPROPERTIES should override source TBLPROPERTIES")
    }
  }

  test("PROP_OWNER is set to current user in TableInfo passed to connector") {
    // Spark sets PROP_OWNER in the TableInfo it passes to createTableLike so that
    // connectors do not need to call a Catalyst utility to determine the owner.
    withTable("testcat.src", "testcat.dst") {
      sql("CREATE TABLE testcat.src (id bigint) USING foo")
      sql("CREATE TABLE testcat.dst LIKE testcat.src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.containsKey(TableCatalog.PROP_OWNER),
        "PROP_OWNER should be set in TableInfo so connectors do not need to infer it")
      assert(dst.properties.get(TableCatalog.PROP_OWNER).nonEmpty)
    }
  }

  test("columns and partitioning from source are set in TableInfo passed to connector") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint, data string) USING parquet PARTITIONED BY (data)")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      val columnNames = dst.columns().map(_.name)
      assert(columnNames === Array("id", "data"))
      assert(dst.partitioning().nonEmpty,
        "partitioning from source should be passed in TableInfo")
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

  test("source provider is copied to v2 target when no USING override") {
    // When no USING clause is given, provider inheritance is handled by the connector:
    // InMemoryTableCatalog.createTableLike merges sourceTable.properties() into the target,
    // which includes PROP_PROVIDER set by the source table.
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

  test("CHAR and VARCHAR types are preserved from v1 source to v2 target") {
    // InMemoryTableCatalog.createTableLike applies CharVarcharUtils.getRawSchema when
    // the source is a V1Table, preserving CHAR/VARCHAR types as declared.
    // This illustrates the pattern connectors should follow to preserve declared types.
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint, name CHAR(10), tag VARCHAR(20)) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      val schema = CatalogV2Util.v2ColumnsToStructType(dst.columns())
      assert(schema("name").dataType === CharType(10))
      assert(schema("tag").dataType === VarcharType(20))
    }
  }

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
  // V2 source, session catalog (V1) target: unsupported
  // -------------------------------------------------------------------------

  test("v2 source, session catalog target: throws because session catalog does not " +
      "implement createTableLike") {
    // CREATE TABLE LIKE targeting the session catalog with a V2 source goes through
    // CreateTableLikeExec, which calls createTableLike on the session catalog.
    // The session catalog (InMemoryTableSessionCatalog in tests) does not override
    // createTableLike, so the default UnsupportedOperationException is thrown.
    withTable("testcat.src") {
      sql("CREATE TABLE testcat.src (id bigint, data string) USING parquet")
      val ex = intercept[UnsupportedOperationException] {
        sql("CREATE TABLE dst LIKE testcat.src")
      }
      assert(ex.getMessage.contains("CREATE TABLE LIKE"))
    }
  }

  // -------------------------------------------------------------------------
  // ROW FORMAT / STORED AS propagation
  // -------------------------------------------------------------------------

  test("STORED AS is converted to hive.stored-as property for v2 target") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql("CREATE TABLE testcat.dst LIKE src STORED AS textfile")

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("hive.stored-as") === "textfile",
        "STORED AS should be converted to hive.stored-as property")
    }
  }

  test("STORED AS INPUTFORMAT/OUTPUTFORMAT are converted to hive properties for v2 target") {
    withTable("src", "testcat.dst") {
      sql("CREATE TABLE src (id bigint) USING parquet")
      sql(
        """CREATE TABLE testcat.dst LIKE src
          |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
          |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
          |""".stripMargin)

      val dst = testCatalog.loadTable(Identifier.of(Array(), "dst"))
      assert(dst.properties.get("hive.input-format") ===
        "org.apache.hadoop.mapred.TextInputFormat",
        "INPUTFORMAT should be converted to hive.input-format property")
      assert(dst.properties.get("hive.output-format") ===
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "OUTPUTFORMAT should be converted to hive.output-format property")
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
