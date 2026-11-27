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

package org.apache.spark.sql.execution.python

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{AnalysisException, DataFrame, Encoders, Row}
import org.apache.spark.sql.IntegratedUDFTestUtils.{createUserDefinedPythonDataSource, shouldTestPandasUDFs}
import org.apache.spark.sql.catalyst.plans.logical.PreparedExternalLookupJoin
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.LookupParameter
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, ExternalLookupRelation}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonExternalLookupReaderFactory, PythonScan, PythonScanBuilder}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{EqualTo, Filter, Or}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Tests of the driver-side planning of external lookups on Python data sources. The lookup
 * reader selects its lookup predicates with the Python expression of the `select` option, the
 * positions of the selected predicates in the list `candidates`, and returns the Python literal
 * of the `maxRows` option, when given, from `maxParameterRowsPerRequest()`.
 */
class PythonDataSourceExternalLookupSuite extends PythonDataSourceSuiteBase {
  private val quotedName = s"`$dataSourceName`"

  // The methods of a batch reader of a data source that serves batch reads and lookups.
  private val batchReaderMethods =
    """
      |    def partitions(self):
      |        return [InputPartition(0)]
      |
      |    def read(self, partition):
      |        yield (1, 0, "one")
      |""".stripMargin

  /**
   * A data source with the schema `id BIGINT, tier INT, name STRING` that serves lookups, and
   * batch reads when `batchReader` holds the methods of its batch reader.
   */
  private def lookupSourceScript(
      batchReader: Option[String] = Some(batchReaderMethods),
      lookupReaderMethods: String = ""): String = {
    val reader = batchReader.map { methods =>
      s"""
         |class TestReader(DataSourceReader):
         |$methods
         |""".stripMargin
    }.getOrElse("")
    val readerMethod = if (batchReader.isDefined) {
      """
        |    def reader(self, schema):
        |        return TestReader()
        |""".stripMargin
    } else {
      ""
    }
    s"""
       |import ast
       |from pyspark.sql.datasource import (
       |    DataSource,
       |    DataSourceLookupReader,
       |    DataSourceReader,
       |    EqualTo,
       |    InputPartition,
       |    LookupParameter,
       |)
       |from pyspark.sql.types import LongType
       |$reader
       |class TestLookupReader(DataSourceLookupReader):
       |    def __init__(self, options):
       |        self.options = options
       |$lookupReaderMethods
       |    def selectLookupPredicates(self, candidates):
       |        select = self.options.get(
       |            "select",
       |            "[i for i, c in enumerate(candidates) if c.attribute == ('id',)][:1]")
       |        return eval(select, {
       |            "candidates": candidates,
       |            "EqualTo": EqualTo,
       |            "LookupParameter": LookupParameter,
       |            "LongType": LongType,
       |        })
       |
       |    def maxParameterRowsPerRequest(self):
       |        if "maxRows" in self.options:
       |            return ast.literal_eval(self.options["maxRows"])
       |        return super().maxParameterRowsPerRequest()
       |
       |    def lookup(self, parameters):
       |        return iter([])
       |
       |class $dataSourceName(DataSource):
       |    def schema(self):
       |        return "id BIGINT, tier INT, name STRING"
       |$readerMethod
       |    def lookupReader(self, schema):
       |        return TestLookupReader(self.options)
       |""".stripMargin
  }

  private def registerLookupSource(script: String): Unit = {
    spark.dataSource.registerPython(
      dataSourceName, createUserDefinedPythonDataSource(dataSourceName, script))
  }

  // The tests plan streaming queries without running them, so the check that a streaming query
  // is run with `writeStream.start()` is turned off.
  private def withExternalLookup(confs: (String, String)*)(f: => Unit): Unit = {
    withSQLConf(Seq(
      SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "true",
      SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") ++ confs: _*)(f)
  }

  private def lookupTable(options: (String, String)*): DataFrame =
    spark.read.format(dataSourceName).options(options.toMap).load()

  /** A streaming DataFrame of the column `value BIGINT`, joined with `lookup` on its `id`. */
  private def joinWithStream(lookup: DataFrame): DataFrame = {
    val stream = MemoryStream[Long](1)(Encoders.scalaLong, spark.sqlContext).toDF()
    stream.join(lookup, stream("value") === lookup("id"))
  }

  private def preparedLookupJoin(df: DataFrame): Option[PreparedExternalLookupJoin] =
    df.queryExecution.optimizedPlan.collectFirst {
      case join: PreparedExternalLookupJoin => join
    }

  private def lookupRelation(df: DataFrame): DataSourceV2Relation =
    df.queryExecution.analyzed.collectFirst {
      case relation: DataSourceV2Relation => relation
      case lookup: ExternalLookupRelation => lookup.relation
    }.get

  private def table(df: DataFrame): Table = lookupRelation(df).table

  private def scanBuilder(df: DataFrame): PythonScanBuilder = {
    val relation = lookupRelation(df)
    relation.table.asInstanceOf[SupportsRead].newScanBuilder(relation.options)
      .asInstanceOf[PythonScanBuilder]
  }

  private def candidate(name: String, column: String, parameter: LookupParameter): Predicate =
    new Predicate(name, Array(FieldReference.column(column), parameter))

  test("a lookup-only Python data source is read by lookups without a hint") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = None))
    withExternalLookup() {
      val lookup = lookupTable()
      assert(lookup.queryExecution.analyzed.isInstanceOf[ExternalLookupRelation])
      assert(table(lookup).capabilities().asScala ===
        Set(EXTERNAL_LOOKUP, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE))

      val joined = joinWithStream(lookup)
      val prepared = preparedLookupJoin(joined).get
      assert(prepared.scan.isInstanceOf[PythonScan])
      assert(prepared.readerFactory.isInstanceOf[PythonExternalLookupReaderFactory])
      // The default of maxParameterRowsPerRequest() is Spark's own cap.
      assert(prepared.maxParameterRowsPerRequest === 1000)

      // A batch read of a lookup-only data source fails.
      checkError(
        exception = intercept[AnalysisException] { lookup.queryExecution.optimizedPlan },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map("tableName" -> quotedName, "operation" -> "batch scan"))
    }
  }

  test("a Python data source that serves batch reads and lookups needs the hint") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript())
    withExternalLookup() {
      val lookup = lookupTable()
      assert(table(lookup).capabilities().asScala === Set(
        BATCH_READ, EXTERNAL_LOOKUP, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE))
      assert(preparedLookupJoin(joinWithStream(lookup)).isEmpty)
      assert(preparedLookupJoin(joinWithStream(lookup.hint("EXTERNAL_LOOKUP"))).isDefined)
      checkAnswer(lookup, Row(1L, 0, "one"))
    }
  }

  test("Python data sources keep their capabilities when external lookups are disabled") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = None))
    withSQLConf(
        SQLConf.PYTHON_EXTERNAL_LOOKUP_ENABLED.key -> "false",
        SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
      val lookup = lookupTable()
      assert(lookup.queryExecution.analyzed.isInstanceOf[DataSourceV2Relation])
      assert(table(lookup).capabilities().asScala ===
        Set(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE))
      assert(preparedLookupJoin(joinWithStream(lookup)).isEmpty)
    }
  }

  test("Spark finds lookup support on the class, without creating the data source") {
    assume(shouldTestPandasUDFs)
    withTempDir { dir =>
      // The data source logs each time that Spark creates it in Python.
      val log = new java.io.File(dir, "created")
      registerLookupSource(lookupSourceScript() +
        s"""
           |    def __init__(self, options):
           |        super().__init__(options)
           |        with open("${log.getAbsolutePath}", "a") as f:
           |            f.write("created\\n")
           |
           |    def writer(self, schema, overwrite):
           |        from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
           |        class Writer(DataSourceWriter):
           |            def write(self, iterator):
           |                return WriterCommitMessage()
           |        return Writer()
           |""".stripMargin)
      def created: Int = if (log.exists()) {
        java.nio.file.Files.readAllLines(log.toPath).size()
      } else {
        0
      }
      withExternalLookup() {
        // A table with a user-given schema reports its capabilities, which include lookups,
        // with no data source created, as before lookups existed.
        val lookup = spark.read.format(dataSourceName).schema("id BIGINT, tier INT, name STRING")
          .load()
        assert(table(lookup).capabilities().contains(EXTERNAL_LOOKUP))
        assert(created === 0)
        // A write creates the data source once, for the write only.
        spark.range(2).selectExpr("id", "0 AS tier", "'a' AS name")
          .write.format(dataSourceName).mode("append").save()
        assert(created === 1)
      }
    }
  }

  test("batch reads of a Python data source that also serves lookups keep their pushdown") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = Some(
      """
        |    def pushFilters(self, filters):
        |        for f in filters:
        |            if f != EqualTo(("tier",), 0):
        |                yield f
        |
        |    def pushLimit(self, limit):
        |        return True
        |""".stripMargin + batchReaderMethods)))
    withExternalLookup(
        SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      def scanMetadata(df: DataFrame): Map[String, String] =
        collectFirst(df.queryExecution.executedPlan) {
          case scan: BatchScanExec => scan.scan.asInstanceOf[PythonScan].getMetaData()
        }.get

      val filtered = lookupTable().filter("tier = 0")
      assert(scanMetadata(filtered)("PushedFilters") === "[EqualTo(tier,0)]")
      checkAnswer(filtered, Row(1L, 0, "one"))
      val limited = lookupTable().limit(1)
      assert(scanMetadata(limited)("PushedLimit") === "LIMIT 1")
      checkAnswer(limited, Row(1L, 0, "one"))
    }
  }

  test("the lookup reader's selection keeps its positions and its order") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = None))
    withExternalLookup() {
      // Two candidates of the same shape, as from `t.id = s.a AND t.id = s.b`, are told apart by
      // position.
      val candidates = Array(
        candidate("=", "id", new LookupParameter(LongType)),
        candidate(">=", "tier", new LookupParameter(IntegerType)),
        candidate("=", "id", new LookupParameter(LongType)))

      val builder = scanBuilder(lookupTable("select" -> "[2, 1]"))
      assert(builder.selectLookupPredicates(candidates).toSeq === Seq(2, 1))

      val lookup = builder.build().toExternalLookup()
      assert(lookup.maxParameterRowsPerRequest() === 1000)
      val factory = lookup.createReaderFactory().asInstanceOf[PythonExternalLookupReaderFactory]
      // The parameter row has one field per selected predicate, in the order of the selection.
      assert(factory.parameterTypes === Seq(LongType, IntegerType))

      // Every offered shape is passed on to Python and back, in the offered order.
      val shapes = Seq("=", "<=>", "<", "<=", ">", ">=").map { name =>
        candidate(name, "id", new LookupParameter(LongType))
      }.toArray
      Seq("EqualTo", "EqualNullSafe", "LessThan", "LessThanOrEqual", "GreaterThan",
          "GreaterThanOrEqual").zipWithIndex.foreach { case (name, position) =>
        val select = s"[i for i, c in enumerate(candidates) if type(c).__name__ == '$name']"
        val selected = scanBuilder(lookupTable("select" -> select)).selectLookupPredicates(shapes)
        assert(selected.toSeq === Seq(position), name)
      }
    }
  }

  test("an invalid selection of lookup predicates fails the query") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = None))
    withExternalLookup() {
      def planningError(select: String, options: (String, String)*): AnalysisException =
        intercept[AnalysisException] {
          joinWithStream(lookupTable(("select" -> select) +: options: _*))
            .queryExecution.optimizedPlan
        }

      checkError(
        exception = planningError("[]"),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
        parameters = Map("table" -> quotedName))
      checkError(
        exception = planningError("[0, 0]"),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.DUPLICATE_SELECTED_PREDICATE",
        parameters = Map("table" -> quotedName, "position" -> "0", "predicate" -> "id = ?"))
      // Spark rejects the selection before it asks for the request size, as for a Scala
      // connector, so a request size that would break the contract does not matter then.
      checkError(
        exception = planningError("[]", "maxRows" -> "None"),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
        parameters = Map("table" -> quotedName))
      checkError(
        exception = planningError("[0, 0]", "maxRows" -> "None"),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.DUPLICATE_SELECTED_PREDICATE",
        parameters = Map("table" -> quotedName, "position" -> "0", "predicate" -> "id = ?"))
      // The contract violations that Python finds are reported with the errors of a Scala
      // connector.
      Seq("1", "-1", "2 ** 40").foreach { position =>
        checkError(
          exception = planningError(s"[$position]"),
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_SELECTED_PREDICATE_POSITION",
          parameters = Map(
            "table" -> quotedName,
            "position" -> (if (position == "2 ** 40") "1099511627776" else position),
            "maxPosition" -> "0"))
      }
      checkError(
        exception = planningError("None"),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT",
        parameters = Map(
          "table" -> quotedName,
          "method" -> "DataSourceLookupReader.selectLookupPredicates()"))
      // Python finds a list of candidates that the reader changed, and Spark describes the
      // candidate as it offered it.
      checkError(
        exception = planningError("(candidates.clear(), [0])[1]"),
        condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.MODIFIED_CANDIDATES",
        parameters = Map("table" -> quotedName, "position" -> "0", "predicate" -> "id = ?"))
      Seq("candidates[0]" -> "EqualTo", "candidates" -> "list holding EqualTo",
          "[True]" -> "list holding bool").foreach { case (select, pythonType) =>
        checkError(
          exception = planningError(select),
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_RESULT_TYPE",
          parameters = Map(
            "table" -> quotedName,
            "method" -> "DataSourceLookupReader.selectLookupPredicates()",
            "type" -> pythonType,
            "expected" -> "a sequence of ints"))
      }
    }
  }

  test("maxParameterRowsPerRequest must return a positive int") {
    assume(shouldTestPandasUDFs)
    registerLookupSource(lookupSourceScript(batchReader = None))
    withExternalLookup() {
      def planned(maxRows: String): DataFrame =
        joinWithStream(lookupTable("maxRows" -> maxRows))

      assert(preparedLookupJoin(planned("5")).get.maxParameterRowsPerRequest === 5)
      Seq("0", "-1").foreach { maxRows =>
        checkError(
          exception = intercept[AnalysisException] {
            planned(maxRows).queryExecution.optimizedPlan
          },
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NON_POSITIVE_MAX_PARAMETER_ROWS",
          parameters = Map("table" -> quotedName, "value" -> maxRows))
      }
      Seq("True" -> "bool", "'5'" -> "str").foreach { case (maxRows, pythonType) =>
        checkError(
          exception = intercept[AnalysisException] {
            planned(maxRows).queryExecution.optimizedPlan
          },
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.INVALID_RESULT_TYPE",
          parameters = Map(
            "table" -> quotedName,
            "method" -> "DataSourceLookupReader.maxParameterRowsPerRequest()",
            "type" -> pythonType,
            "expected" -> "an int"))
      }
    }
  }

  // The methods of a lookup reader that pushes `tier = 2`.
  private val tierPushingLookupReader =
    """
      |    def pushFilters(self, filters):
      |        for f in filters:
      |            if f != EqualTo(("tier",), 2):
      |                yield f
      |""".stripMargin

  private def pushedFilters(df: DataFrame): String =
    preparedLookupJoin(joinWithStream(df)).get.scan.asInstanceOf[PythonScan]
      .getMetaData()("PushedFilters")

  test("filters on the lookup side are pushed into the lookup reader") {
    assume(shouldTestPandasUDFs)
    // The batch reader pushes no filter, and the lookup reader pushes `tier = 2`, so the scan
    // reports the filters that the lookup reader pushes.
    registerLookupSource(lookupSourceScript(
      batchReader = Some(
        """
          |    def pushFilters(self, filters):
          |        return filters
          |""".stripMargin + batchReaderMethods),
      lookupReaderMethods = tierPushingLookupReader))
    withExternalLookup(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      assert(pushedFilters(lookupTable().hint("EXTERNAL_LOOKUP").filter("tier = 2")) ===
        "[EqualTo(tier,2)]")
    }
    // Also with limit pushdown on, which defers the planning of the batch read: the lookup scan
    // plans no batch read, so the batch reader's other answer is never replayed.
    withExternalLookup(
        SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      assert(pushedFilters(lookupTable().hint("EXTERNAL_LOOKUP").filter("tier = 2")) ===
        "[EqualTo(tier,2)]")
    }
    // A lookup reader that implements pushFilters() fails while filter pushdown is disabled.
    withExternalLookup(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          joinWithStream(lookupTable().hint("EXTERNAL_LOOKUP")).queryExecution.optimizedPlan
        },
        condition = "PYTHON_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "plan",
          "type" -> "lookup",
          "msg" -> "(?s).*DATA_SOURCE_PUSHDOWN_DISABLED.*TestLookupReader.*pushFilters.*"),
        matchPVals = true)
    }
  }

  test("filters on the lookup side of a lookup-only data source are pushed into its reader") {
    assume(shouldTestPandasUDFs)
    // A lookup-only data source has no batch reader, so its filters go to the lookup reader
    // only, also with filter pushdown on.
    registerLookupSource(
      lookupSourceScript(batchReader = None, lookupReaderMethods = tierPushingLookupReader))
    withExternalLookup(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      assert(pushedFilters(lookupTable().filter("tier = 2")) === "[EqualTo(tier,2)]")

      // A filter that Python does not receive, ahead of `tier = 2`, does not shift the pushed
      // filters, which refer to the positions of all the filters.
      val builder = scanBuilder(lookupTable())
      val filters = Array[Filter](
        Or(EqualTo("tier", 1), EqualTo("name", "a")), EqualTo("tier", 2))
      assert(builder.pushFilters(filters).toSeq === filters.toSeq)
      builder.selectLookupPredicates(Array(candidate("=", "id", new LookupParameter(LongType))))
      assert(builder.pushedFilters().toSeq === Seq(EqualTo("tier", 2)))
    }
  }
}
