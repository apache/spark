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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTable.{VIEW_SCHEMA_MODE, VIEW_SQL_CONFIG_PREFIX}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog, InMemoryViewCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CreateViewV2Suite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  private val catalogName = "testviewcat"
  private val ns = Array("ns")

  private def viewCatalog: InMemoryViewCatalog =
    spark.sessionState.catalogManager.catalog(catalogName).asInstanceOf[InMemoryViewCatalog]

  before {
    spark.conf.set(s"spark.sql.catalog.$catalogName", classOf[InMemoryViewCatalog].getName)
    sql(s"CREATE NAMESPACE $catalogName.ns")
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalogName")
  }

  // --------------------------------------------------------------------------
  // Basic CREATE VIEW
  // --------------------------------------------------------------------------

  test("create view in v2 catalog") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")

      val ident = Identifier.of(ns, "v")
      val view = viewCatalog.loadView(ident)
      assert(view.query() == "SELECT 1 AS id")
      assert(view.schema().fieldNames.toSeq == Seq("id"))
    }
  }

  test("create view with column aliases") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v (col1 COMMENT 'first') AS SELECT 1")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.columnAliases().toSeq == Seq("col1"))
      assert(view.columnComments().toSeq == Seq("first"))
      assert(view.schema().fieldNames.toSeq == Seq("col1"))
    }
  }

  test("create view with TBLPROPERTIES") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v TBLPROPERTIES ('k' = 'v') AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.properties().get("k") == "v")
    }
  }

  test("create view with COMMENT") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v COMMENT 'a test view' AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.properties().get("comment") == "a test view")
    }
  }

  // --------------------------------------------------------------------------
  // IF NOT EXISTS
  // --------------------------------------------------------------------------

  test("create view IF NOT EXISTS: no-op when view already exists") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")
      sql(s"CREATE VIEW IF NOT EXISTS $catalogName.ns.v AS SELECT 2 AS id")

      // Original view SQL should be unchanged.
      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.query() == "SELECT 1 AS id")
    }
  }

  test("create view without IF NOT EXISTS: fails when view already exists") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")
      val e = intercept[ViewAlreadyExistsException] {
        sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 2 AS id")
      }
      assert(e.getMessage.contains("v"))
    }
  }

  // --------------------------------------------------------------------------
  // CREATE OR REPLACE VIEW
  // --------------------------------------------------------------------------

  test("create or replace view: replaces an existing view") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")
      sql(s"CREATE OR REPLACE VIEW $catalogName.ns.v AS SELECT 2 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.query() == "SELECT 2 AS id")
    }
  }

  test("create or replace view: creates when view does not exist") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE OR REPLACE VIEW $catalogName.ns.v AS SELECT 42 AS x")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.query() == "SELECT 42 AS x")
    }
  }

  // --------------------------------------------------------------------------
  // currentCatalog / currentNamespace captured
  // --------------------------------------------------------------------------

  test("currentCatalog and currentNamespace are stored in view metadata") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.currentCatalog() == spark.sessionState.catalogManager.currentCatalog.name())
      assert(view.currentNamespace() != null)
    }
  }

  // --------------------------------------------------------------------------
  // Non-ViewCatalog V2 catalog: should still throw
  // --------------------------------------------------------------------------

  test("create view in a non-ViewCatalog v2 catalog throws an error") {
    val tableOnlyCat = "tableonlycat"
    try {
      spark.conf.set(s"spark.sql.catalog.$tableOnlyCat", classOf[InMemoryTableCatalog].getName)
      val e = intercept[Exception] {
        sql(s"CREATE VIEW $tableOnlyCat.v AS SELECT 1 AS id")
      }
      assert(e.getMessage.contains("does not support views") ||
        e.getMessage.contains("missingCatalogViewsAbility") ||
        e.getMessage.contains(tableOnlyCat))
    } finally {
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$tableOnlyCat")
    }
  }

  // --------------------------------------------------------------------------
  // viewSchemaMode stored in properties
  // --------------------------------------------------------------------------

  test("create view without schema mode: no view.schemaMode property stored") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(!view.properties().containsKey(VIEW_SCHEMA_MODE))
    }
  }

  test("create view WITH SCHEMA BINDING: stores view.schemaMode = BINDING") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v WITH SCHEMA BINDING AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.properties().get(VIEW_SCHEMA_MODE) == "BINDING")
    }
  }

  test("create view WITH SCHEMA EVOLUTION: stores view.schemaMode = EVOLUTION") {
    withView(s"$catalogName.ns.v") {
      sql(s"CREATE VIEW $catalogName.ns.v WITH SCHEMA EVOLUTION AS SELECT 1 AS id")

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      assert(view.properties().get(VIEW_SCHEMA_MODE) == "EVOLUTION")
    }
  }

  // --------------------------------------------------------------------------
  // SQL configs captured at creation time
  // --------------------------------------------------------------------------

  test("SQL configs at creation time are stored in view properties") {
    withView(s"$catalogName.ns.v") {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")
      }

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      val tzKey = VIEW_SQL_CONFIG_PREFIX + SQLConf.SESSION_LOCAL_TIMEZONE.key
      assert(view.properties().get(tzKey) == "America/Los_Angeles")
    }
  }

  test("ANSI mode is always captured in view properties") {
    withView(s"$catalogName.ns.v") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        sql(s"CREATE VIEW $catalogName.ns.v AS SELECT 1 AS id")
      }

      val view = viewCatalog.loadView(Identifier.of(ns, "v"))
      val ansiKey = VIEW_SQL_CONFIG_PREFIX + SQLConf.ANSI_ENABLED.key
      assert(view.properties().get(ansiKey) == "true")
    }
  }

  // --------------------------------------------------------------------------
  // Temp-view reference rejected
  // --------------------------------------------------------------------------

  // TODO (SPARK-33903 follow-up): permanent V2 views that reference temp views should be
  // rejected, but the optimizer currently inlines the temp view body before
  // CreateViewExec.run() has a chance to validate. The fix requires preventing optimizer
  // rules from traversing into CreateView.query when targeting a ViewCatalog (similar to
  // the AnalysisOnlyCommand mechanism used in the V1 path).
  ignore("create permanent view referencing a temp view is rejected") {
    withTempView("tmp") {
      sql("CREATE TEMPORARY VIEW tmp AS SELECT 1 AS x")
      val e = intercept[Exception] {
        sql(s"CREATE VIEW $catalogName.ns.v AS SELECT * FROM tmp")
      }
      assert(e.getMessage.contains("tmp") || e.getMessage.contains("temporary"))
    }
  }
}
