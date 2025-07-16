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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Test suite for resolving the flows in a [[DataflowGraph]]. These
 * examples are all semantically correct but contain logical errors which should be found
 * when connect is called and thrown when validate() is called.
 */
class ConnectInvalidPipelineSuite extends PipelineTest with SharedSparkSession {

  test("Missing source") {
    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("b", query = readFlowFunc("a"))
    }

    val dfg = new P().resolveToDataflowGraph()
    assert(!dfg.resolved, "Pipeline should not have resolved properly")
    val ex = intercept[UnresolvedPipelineException] {
      dfg.validate()
    }
    assert(ex.getMessage.contains("Failed to resolve flows in the pipeline"))
    assertAnalysisException(
      ex.directFailures(fullyQualifiedIdentifier("b")),
      "TABLE_OR_VIEW_NOT_FOUND"
    )
  }

  test("Correctly differentiate between upstream and downstream errors") {
    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(spark.range(5).toDF()))
      registerPersistedView("b", query = readFlowFunc("nonExistentFlow"))
      registerPersistedView("c", query = readFlowFunc("b"))
      registerPersistedView("d", query = dfFlowFunc(spark.range(5).toDF()))
      registerPersistedView(
        "e",
        query = sqlFlowFunc(spark, "SELECT nonExistentColumn FROM RANGE(5)")
      )
      registerPersistedView("f", query = readFlowFunc("e"))
    }

    val dfg = new P().resolveToDataflowGraph()
    assert(!dfg.resolved, "Pipeline should not have resolved properly")
    val ex = intercept[UnresolvedPipelineException] {
      dfg.validate()
    }
    assert(ex.getMessage.contains("Failed to resolve flows in the pipeline"))
    assert(
      ex.getMessage.contains(
        s"Flows with errors: " +
        s"${fullyQualifiedIdentifier("b").unquotedString}," +
        s" ${fullyQualifiedIdentifier("e").unquotedString}"
      )
    )
    assert(
      ex.getMessage.contains(
        s"Flows that failed due to upstream errors: " +
        s"${fullyQualifiedIdentifier("c").unquotedString}, " +
        s"${fullyQualifiedIdentifier("f").unquotedString}"
      )
    )
    assert(
      ex.directFailures.keySet == Set(
        fullyQualifiedIdentifier("b"),
        fullyQualifiedIdentifier("e")
      )
    )
    assert(
      ex.downstreamFailures.keySet == Set(
        fullyQualifiedIdentifier("c"),
        fullyQualifiedIdentifier("f")
      )
    )
    assertAnalysisException(
      ex.directFailures(fullyQualifiedIdentifier("b")),
      "TABLE_OR_VIEW_NOT_FOUND"
    )
    assert(
      ex.directFailures(fullyQualifiedIdentifier("e"))
        .isInstanceOf[AnalysisException]
    )
    assert(
      ex.directFailures(fullyQualifiedIdentifier("e"))
        .getMessage
        .contains("nonExistentColumn")
    )
    assert(
      ex.downstreamFailures(fullyQualifiedIdentifier("c"))
        .isInstanceOf[UnresolvedDatasetException]
    )
    assert(
      ex.downstreamFailures(fullyQualifiedIdentifier("c"))
        .getMessage
        .contains(
          s"Failed to read dataset " +
          s"'${fullyQualifiedIdentifier("b").unquotedString}'. " +
          s"Dataset is defined in the pipeline but could not be resolved"
        )
    )
    assert(
      ex.downstreamFailures(fullyQualifiedIdentifier("f"))
        .isInstanceOf[UnresolvedDatasetException]
    )
    assert(
      ex.downstreamFailures(fullyQualifiedIdentifier("f"))
        .getMessage
        .contains(
          s"Failed to read dataset " +
          s"'${fullyQualifiedIdentifier("e").unquotedString}'. " +
          s"Dataset is defined in the pipeline but could not be resolved"
        )
    )
  }

  test("correctly identify direct and downstream errors for multi-flow pipelines") {
    class P extends TestGraphRegistrationContext(spark) {
      registerTable("a")
      registerFlow("a", "a", dfFlowFunc(spark.range(5).toDF()))
      registerFlow("a", "a_2", sqlFlowFunc(spark, "SELECT non_existent_col FROM RANGE(5)"))
      registerTable("b", query = Option(readFlowFunc("a")))
    }
    val ex = intercept[UnresolvedPipelineException] { new P().resolveToDataflowGraph().validate() }
    assert(ex.directFailures.keySet == Set(fullyQualifiedIdentifier("a_2")))
    assert(ex.downstreamFailures.keySet == Set(fullyQualifiedIdentifier("b")))

  }

  test("Missing attribute in the schema") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("z")))
      registerPersistedView("b", query = sqlFlowFunc(spark, "SELECT x FROM a"))
    }

    val dfg = new P().resolveToDataflowGraph()
    val ex = intercept[UnresolvedPipelineException] {
      dfg.validate()
    }.directFailures(fullyQualifiedIdentifier("b")).getMessage
    verifyUnresolveColumnError(ex, "x", Seq("z"))
  }

  test("Joining on a column with different names") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("b", query = dfFlowFunc(Seq("a", "b", "c").toDF("y")))
      registerPersistedView("c", query = sqlFlowFunc(spark, "SELECT * FROM a JOIN b USING (x)"))
    }

    val dfg = new P().resolveToDataflowGraph()
    val ex = intercept[UnresolvedPipelineException] {
      dfg.validate()
    }
    assert(
      ex.directFailures(fullyQualifiedIdentifier("c"))
        .getMessage
        .contains("USING column `x` cannot be resolved on the right side")
    )
  }

  test("Writing to one table by unioning flows with different schemas") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("b", query = dfFlowFunc(Seq(true, false).toDF("x")))
      registerPersistedView(
        "c",
        query = sqlFlowFunc(spark, "SELECT x FROM a UNION SELECT x FROM b")
      )
    }

    val dfg = new P().resolveToDataflowGraph()
    assert(!dfg.resolved)
    val ex = intercept[UnresolvedPipelineException] {
      dfg.validate()
    }
    assert(
      ex.directFailures(fullyQualifiedIdentifier("c"))
        .getMessage
        .contains("compatible column types") ||
        ex.directFailures(fullyQualifiedIdentifier("c"))
          .getMessage
          .contains("Failed to merge incompatible data types")
    )
  }

  test("Self reference") {
    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = readFlowFunc("a"))
    }
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    assert(e.upstreamDataset == fullyQualifiedIdentifier("a"))
    assert(e.downstreamTable == fullyQualifiedIdentifier("a"))
  }

  test("Cyclic graph - simple") {
    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = readFlowFunc("b"))
      registerPersistedView("b", query = readFlowFunc("a"))
    }
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    val cycle = Set(
      fullyQualifiedIdentifier("a"),
      fullyQualifiedIdentifier("b")
    )
    assert(e.upstreamDataset != e.downstreamTable)
    assert(cycle.contains(e.upstreamDataset))
    assert(cycle.contains(e.downstreamTable))
  }

  test("Cyclic graph") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView(
        "b",
        query = sqlFlowFunc(spark, "SELECT * FROM a UNION SELECT * FROM d")
      )
      registerPersistedView("c", query = readFlowFunc("b"))
      registerPersistedView("d", query = readFlowFunc("c"))
    }
    val cycle =
      Set(
        fullyQualifiedIdentifier("b"),
        fullyQualifiedIdentifier("c"),
        fullyQualifiedIdentifier("d")
      )
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    assert(e.upstreamDataset != e.downstreamTable)
    assert(cycle.contains(e.upstreamDataset))
    assert(cycle.contains(e.downstreamTable))
  }

  test("Cyclic graph with materialized nodes") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(Seq(1, 2, 3).toDF("x"))))
      registerTable(
        "b",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM a UNION SELECT * FROM d"))
      )
      registerTable("c", query = Option(readFlowFunc("b")))
      registerTable("d", query = Option(readFlowFunc("c")))
    }
    val cycle =
      Set(
        fullyQualifiedIdentifier("b"),
        fullyQualifiedIdentifier("c"),
        fullyQualifiedIdentifier("d")
      )
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    assert(e.upstreamDataset != e.downstreamTable)
    assert(cycle.contains(e.upstreamDataset))
    assert(cycle.contains(e.downstreamTable))
  }

  test("Cyclic graph - second query makes it cyclic") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(Seq(1, 2, 3).toDF("x"))))
      registerTable("b")
      registerFlow("b", "b", readFlowFunc("a"))
      registerFlow("b", "b2", readFlowFunc("d"))
      registerTable("c", query = Option(readFlowFunc("b")))
      registerTable("d", query = Option(readFlowFunc("c")))
    }
    val cycle =
      Set(
        fullyQualifiedIdentifier("b"),
        fullyQualifiedIdentifier("c"),
        fullyQualifiedIdentifier("d")
      )
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    assert(e.upstreamDataset != e.downstreamTable)
    assert(cycle.contains(e.upstreamDataset))
    assert(cycle.contains(e.downstreamTable))
  }

  test("Cyclic graph - all named queries") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(Seq(1, 2, 3).toDF("x"))))
      registerTable("b")
      registerFlow("b", "`b-name`", sqlFlowFunc(spark, "SELECT * FROM a UNION SELECT * FROM d"))
      registerTable("c")
      registerFlow("c", "`c-name`", readFlowFunc("b"))
      registerTable("d")
      registerFlow("d", "`d-name`", readFlowFunc("c"))
    }
    val cycle =
      Set(
        fullyQualifiedIdentifier("b"),
        fullyQualifiedIdentifier("c"),
        fullyQualifiedIdentifier("d")
      )
    val e = intercept[CircularDependencyException] {
      new P().resolveToDataflowGraph().validate()
    }
    assert(e.upstreamDataset != e.downstreamTable)
    assert(cycle.contains(e.upstreamDataset))
    assert(cycle.contains(e.downstreamTable))
  }

  test("view-table conf conflict") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1).toDF()), sqlConf = Map("x" -> "a-val"))
      registerTable("b", query = Option(readFlowFunc("a")), sqlConf = Map("x" -> "b-val"))
    }
    val ex = intercept[AnalysisException] { p.resolveToDataflowGraph() }
    assert(
      ex.getMessage.contains(
        s"Found duplicate sql conf for dataset " +
        s"'${fullyQualifiedIdentifier("b").unquotedString}':"
      )
    )
    assert(
      ex.getMessage.contains(
        s"'x' is defined by both " +
        s"'${fullyQualifiedIdentifier("a").unquotedString}' " +
        s"and '${fullyQualifiedIdentifier("b").unquotedString}'"
      )
    )
  }

  test("view-view conf conflict") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1).toDF()), sqlConf = Map("x" -> "a-val"))
      registerPersistedView("b", query = dfFlowFunc(Seq(1).toDF()), sqlConf = Map("x" -> "b-val"))
      registerTable(
        "c",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM a UNION SELECT * FROM b")),
        sqlConf = Map("y" -> "c-val")
      )
    }
    val ex = intercept[AnalysisException] { p.resolveToDataflowGraph() }
    assert(
      ex.getMessage.contains(
        s"Found duplicate sql conf for dataset " +
        s"'${fullyQualifiedIdentifier("c").unquotedString}':"
      )
    )
    assert(
      ex.getMessage.contains(
        s"'x' is defined by both " +
        s"'${fullyQualifiedIdentifier("a").unquotedString}' " +
        s"and '${fullyQualifiedIdentifier("b").unquotedString}'"
      )
    )
  }

  test("reading a complete view incrementally") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1).toDF()))
      registerTable("b", query = Option(readStreamFlowFunc("a")))
    }
    val ex = intercept[UnresolvedPipelineException] { p.resolveToDataflowGraph().validate() }
    assert(
      ex.directFailures(fullyQualifiedIdentifier("b"))
        .getMessage
        .contains(
          s"View ${fullyQualifiedIdentifier("a").quotedString}" +
          s" is a batch view and must be referenced using SparkSession#read."
        )
    )
  }

  test("reading an incremental view completely") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      val mem = MemoryStream[Int]
      mem.addData(1)
      registerPersistedView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("b", query = Option(readFlowFunc("a")))
    }
    val ex = intercept[UnresolvedPipelineException] { p.resolveToDataflowGraph().validate() }
    assert(
      ex.directFailures(fullyQualifiedIdentifier("b"))
        .getMessage
        .contains(
          s"View ${fullyQualifiedIdentifier("a").quotedString} " +
          s"is a streaming view and must be referenced using SparkSession#readStream"
        )
    )
  }

  test("Streaming table backed by batch relation fails validation") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      registerTable("a", query = Option(dfFlowFunc(Seq(1, 2).toDF())))
    }.resolveToDataflowGraph()

    val ex = intercept[AnalysisException] {
      graph.validate()
    }

    checkError(
      exception = ex,
      condition = "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE",
      parameters = Map(
        "flowIdentifier" -> fullyQualifiedIdentifier("a").quotedString,
        "tableIdentifier" -> fullyQualifiedIdentifier("a").quotedString
      )
    )
  }

  test("Materialized view backed by streaming relation fails validation") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      registerMaterializedView("a", query = dfFlowFunc(MemoryStream[Int].toDF()))
    }.resolveToDataflowGraph()

    val ex = intercept[AnalysisException] {
      graph.validate()
    }

    checkError(
      exception = ex,
      condition = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW",
      parameters = Map(
        "flowIdentifier" -> fullyQualifiedIdentifier("a").quotedString,
        "tableIdentifier" -> fullyQualifiedIdentifier("a").quotedString
      )
    )
  }

  test("Once flow backed by streaming relation fails validation") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      registerTable("a")
      registerFlow(
        destinationName = "a",
        name = "once_flow",
        query = dfFlowFunc(MemoryStream[Int].toDF()),
        once = true
      )
    }.resolveToDataflowGraph()

    val ex = intercept[AnalysisException] {
      graph.validate()
    }

    checkError(
      exception = ex,
      condition = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_ONCE_FLOW",
      parameters = Map(
        "flowIdentifier" -> fullyQualifiedIdentifier("once_flow").quotedString
      )
    )
  }

  test("Inferred schema that isn't a subset of user-specified schema") {
    val session = spark
    import session.implicits._

    val graph1 = new TestGraphRegistrationContext(spark) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(Seq(1, 2).toDF("incorrect-col-name"))),
        specifiedSchema = Option(new StructType().add("x", IntegerType))
      )
    }.resolveToDataflowGraph()
    val ex1 = intercept[AnalysisException] { graph1.validate() }
    assert(
      ex1.getMessage.contains(
        s"'${fullyQualifiedIdentifier("a").unquotedString}' " +
        s"has a user-specified schema that is incompatible"
      )
    )
    assert(ex1.getMessage.contains("incorrect-col-name"))

    val graph2 = new TestGraphRegistrationContext(spark) {
      registerTable("a", specifiedSchema = Option(new StructType().add("x", IntegerType)))
      registerFlow("a", "a", query = dfFlowFunc(Seq(true, false).toDF("x")), once = true)
    }.resolveToDataflowGraph()
    val ex2 = intercept[AnalysisException] { graph2.validate() }
    assert(
      ex2.getMessage.contains(
        s"'${fullyQualifiedIdentifier("a").unquotedString}' " +
        s"has a user-specified schema that is incompatible"
      )
    )
    assert(ex2.getMessage.contains("boolean") && ex2.getMessage.contains("integer"))

    val streamingTableHint = "please full refresh"
    assert(!ex1.getMessage.contains(streamingTableHint))
    assert(ex2.getMessage.contains(streamingTableHint))
  }
}
