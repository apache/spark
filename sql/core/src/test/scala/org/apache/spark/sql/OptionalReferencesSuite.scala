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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.util.{GeneratedColumn, ResolveDefaultColumns}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.metricview.serde.{
  AssetSource,
  Column => MetricViewColumn,
  DimensionExpression,
  MeasureExpression,
  MetricView,
  MetricViewFactory,
  Source,
  SQLSource
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

/**
 * Tests of optional references (`x?`) that the golden file `optional-references.sql`, which covers
 * the queries, cannot express: a second analysis of the same unresolved plan, a query that only the
 * fixed-point analyzer resolves, and errors whose context is relative to text that is parsed apart
 * from the statement, such as the parameter list of a SQL function and a metric view definition.
 */
class OptionalReferencesSuite extends QueryTest with SharedSparkSession {

  test("SPARK-61494: analyzing the same unresolved plan again resolves a field that appeared") {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true") {
      withTempView("t") {
        def defineT(fields: String): Unit = {
          sql(s"CREATE OR REPLACE TEMPORARY VIEW t AS SELECT id, array(named_struct($fields)) " +
            "AS arr FROM VALUES (1), (2) AS v(id)")
        }
        // ORDER BY resolves `arr[0].b?` against the SELECT list first, whose `arr` has no field
        // `b`, and then against `t.arr`.
        val sqlText = "SELECT id, array(named_struct('a', 1)) AS arr FROM t ORDER BY arr[0].b?, id"
        val plan = spark.sessionState.sqlParser.parsePlan(sqlText)

        // Neither `arr` has `b`: the rows are sorted by NULL, then by `id`.
        defineT("'a', id")
        checkAnswer(Dataset.ofRows(spark, plan).select("id"), Seq(Row(1), Row(2)))

        // `t.arr` gains `b`. The same unresolved plan sorts by it, like a newly parsed plan would:
        // the first analysis did not record its decision on the plan.
        defineT("'a', id, 'b', 3 - id")
        val reanalyzed = Dataset.ofRows(spark, plan)
        checkAnswer(reanalyzed.select("id"), Seq(Row(2), Row(1)))
        val sortKey = reanalyzed.queryExecution.analyzed.collectFirst {
          case sort: Sort => sort.order.head.child
        }
        assert(sortKey.exists(!_.isInstanceOf[Literal]), s"sorted by $sortKey")
        val fresh = Dataset.ofRows(spark, spark.sessionState.sqlParser.parsePlan(sqlText))
        checkAnswer(reanalyzed, fresh.collect())
      }
    }
  }

  test("SPARK-61494: a HAVING column that is not grouped keeps its optional name parts") {
    // `s.y` first resolves to the column `y` of `s`, which is not grouped, so HAVING resolves the
    // name again against the SELECT list, where `s` is a struct without a field `y`. The optional
    // marker must survive that step. The single-pass resolver does not resolve such a qualified
    // name against the SELECT list, with or without `?`, so this runs the fixed-point analyzer.
    withSQLConf(
      SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "false",
      SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
      val query = "SELECT named_struct('x', 1) AS s FROM VALUES (1, 2) AS s(k, y) GROUP BY k " +
        "HAVING s.y%s IS NULL"
      checkAnswer(sql(query.format("?")), Row(Row(1)))
      assert(intercept[AnalysisException](sql(query.format(""))).getCondition == "FIELD_NOT_FOUND")
    }
  }

  test("SPARK-61494: a metric view definition rejects optional references") {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true") {
      withTable("mv_source") {
        sql("CREATE TABLE mv_source USING parquet AS SELECT * FROM VALUES ('a', 1), ('a', 2) " +
          "AS t(region, count)")
        def createMetricView(
            source: Source = AssetSource("mv_source"),
            where: Option[String] = None,
            dimension: String = "region",
            measure: String = "sum(count)"): String = {
          val yaml = MetricViewFactory.toYAML(MetricView(
            "0.1",
            source,
            where,
            Seq(
              MetricViewColumn("d", DimensionExpression(dimension), 0),
              MetricViewColumn("m", MeasureExpression(measure), 1))))
          s"CREATE VIEW mv WITH METRICS LANGUAGE YAML AS $$$$\n$yaml\n$$$$"
        }
        // The definition is resolved as a part of each query that reads the metric view, so a
        // missing reference could resolve differently for each reader.
        Seq(
          createMetricView(dimension = "missing?") -> "missing?",
          createMetricView(measure = "sum(missing?)") -> "missing?",
          createMetricView(where = Some("missing? IS NULL")) -> "missing?",
          createMetricView(source = SQLSource("SELECT region, count, missing? FROM mv_source")) ->
            "missing?",
          s"BEGIN lbl: BEGIN ${createMetricView(dimension = "lbl.missing?")}; END; END" ->
            "lbl.missing?"
        ).foreach { case (sqlText, reference) =>
          val e = intercept[AnalysisException](sql(sqlText))
          assert(e.getCondition == "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DDL", e.getMessage)
          assert(e.getMessageParameters.get("context") == "METRIC VIEW", e.getMessage)
          assert(e.getQueryContext.map(_.fragment).toSeq == Seq(reference), e.getMessage)
          assert(!spark.catalog.tableExists("mv"))
        }
      }
    }
  }

  test("SPARK-61494: optional references are not supported in the parameters of a SQL function") {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true") {
      // The DEFAULT of a parameter is stored with the function, like the DEFAULT of a column. The
      // parameter list is parsed from its own text, so the context is relative to it.
      val sqlText = "CREATE TEMPORARY FUNCTION f(x INT DEFAULT a?) RETURNS INT RETURN x"
      checkError(
        exception = intercept[ParseException](sql(sqlText)),
        condition = "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DDL",
        sqlState = "0A000",
        parameters = Map("context" -> "DEFAULT"),
        context = ExpectedContext(fragment = "a?", start = 14, stop = 15))
    }
  }

  test("SPARK-61494: a column definition of the Catalog API rejects optional references") {
    withSQLConf(SQLConf.OPTIONAL_REFERENCES_ENABLED.key -> "true") {
      def checkRejected(field: StructField, clause: String, reference: String): Unit = {
        withTable("t") {
          val e = intercept[AnalysisException] {
            spark.catalog.createTable(
              "t", "parquet", StructType(Seq(field)), Map.empty[String, String])
          }
          assert(e.getCondition == "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DDL", e.getMessage)
          assert(e.getMessageParameters.get("context") == clause, e.getMessage)
          assert(e.getQueryContext.map(_.fragment).toSeq == Seq(reference), e.getMessage)
          assert(!spark.catalog.tableExists("t"))
        }
      }
      // The API parses the expressions of a column on their own, so the parser does not see the
      // clause that they belong to.
      checkRejected(
        StructField("v", IntegerType)
          .withCurrentDefaultValue("missing?")
          .withExistenceDefaultValue("NULL"),
        "DEFAULT",
        "missing?")
      checkRejected(
        StructField("v", IntegerType, metadata = new MetadataBuilder()
          .putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, "missing? + 1")
          .build()),
        "GENERATED ALWAYS AS",
        "missing?")

      // So does a default that is stored in the metadata of a column when it is analyzed.
      val e = intercept[AnalysisException] {
        ResolveDefaultColumns.analyze(
          StructField("v", IntegerType).withCurrentDefaultValue("coalesce(missing?, 1)"), "INSERT")
      }
      assert(e.getCondition == "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DDL", e.getMessage)
      assert(e.getMessageParameters.get("context") == "DEFAULT", e.getMessage)
      assert(e.getQueryContext.map(_.fragment).toSeq == Seq("missing?"), e.getMessage)
    }
  }
}
