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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Test suite for resolving the flows in a [[DataflowGraph]]. These
 * examples are all semantically correct and logically correct and connect should not result in any
 * errors.
 */
class ConnectValidPipelineSuite extends PipelineTest with SharedSparkSession {
  test("Extra simple") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("b", query = dfFlowFunc(Seq(1, 2, 3).toDF("y")))
    }
    val p = new P().resolveToDataflowGraph()
    val outSchema = new StructType().add("y", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b"), outSchema)
  }

  test("Simple") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("b", query = sqlFlowFunc(spark, "SELECT x as y FROM a"))
    }
    val p = new P().resolveToDataflowGraph()
    verifyFlowSchema(
      p,
      fullyQualifiedIdentifier("a"),
      new StructType().add("x", IntegerType, false)
    )
    val outSchema = new StructType().add("y", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b"), outSchema)
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("b")).inputs == Set(
        fullyQualifiedIdentifier("a")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("Dependencies") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("c", query = sqlFlowFunc(spark, "SELECT y as z FROM b"))
      registerPersistedView("b", query = sqlFlowFunc(spark, "SELECT x as y FROM a"))
    }
    val p = new P().resolveToDataflowGraph()
    val schemaAB = new StructType().add("y", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b"), schemaAB)
    val schemaBC = new StructType().add("z", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("c"), schemaBC)
  }

  test("Multi-hop schema merging") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView(
        "b",
        query = sqlFlowFunc(spark, """SELECT * FROM VALUES ((1)) OUTER JOIN d ON false""")
      )
      registerPersistedView("e", query = readFlowFunc("b"))
      registerPersistedView("d", query = dfFlowFunc(Seq(1).toDF("y")))
    }
    val p = new P().resolveToDataflowGraph()
    val schemaE = new StructType().add("col1", IntegerType, false).add("y", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b"), schemaE)
    verifyFlowSchema(p, fullyQualifiedIdentifier("e"), schemaE)
  }

  test("Cross product join merges schema") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("b", query = dfFlowFunc(Seq(4, 5, 6).toDF("y")))
      registerPersistedView("c", query = sqlFlowFunc(spark, "SELECT * FROM a CROSS JOIN b"))
    }
    val p = new P().resolveToDataflowGraph()
    val schemaC = new StructType().add("x", IntegerType, false).add("y", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("c"), schemaC)
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("c")).inputs == Set(
        fullyQualifiedIdentifier("a"),
        fullyQualifiedIdentifier("b")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("Real join merges schema") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView(
        "a",
        query = dfFlowFunc(Seq((1, "a"), (2, "b"), (3, "c")).toDF("x", "y"))
      )
      registerPersistedView(
        "b",
        query = dfFlowFunc(Seq((2, "m"), (3, "n"), (4, "o")).toDF("x", "z"))
      )
      registerPersistedView("c", query = sqlFlowFunc(spark, "SELECT * FROM a JOIN b USING (x)"))
    }
    val p = new P().resolveToDataflowGraph()
    val schemaC = new StructType()
      .add("x", IntegerType, false)
      .add("y", StringType)
      .add("z", StringType)
    verifyFlowSchema(p, fullyQualifiedIdentifier("c"), schemaC)
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("c")).inputs == Set(
        fullyQualifiedIdentifier("a"),
        fullyQualifiedIdentifier("b")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("Union of streaming and batch Dataframes") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      val ints = MemoryStream[Int]
      ints.addData(1, 2, 3, 4)
      registerPersistedView("a", query = dfFlowFunc(ints.toDF()))
      registerPersistedView("b", query = dfFlowFunc(Seq(1, 2, 3).toDF()))
      registerPersistedView(
        "c",
        query = FlowAnalysis.createFlowFunctionFromLogicalPlan(
          Union(
            Seq(
              UnresolvedRelation(
                TableIdentifier("a"),
                extraOptions = CaseInsensitiveStringMap.empty(),
                isStreaming = true
              ),
              UnresolvedRelation(TableIdentifier("b"))
            )
          )
        )
      )
    }

    val p = new P().resolveToDataflowGraph()
    verifyFlowSchema(
      p,
      fullyQualifiedIdentifier("c"),
      new StructType().add("value", IntegerType, false)
    )
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("c")).inputs == Set(
        fullyQualifiedIdentifier("a"),
        fullyQualifiedIdentifier("b")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("Union of two streaming Dataframes") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      val ints1 = MemoryStream[Int]
      ints1.addData(1, 2, 3, 4)
      val ints2 = MemoryStream[Int]
      ints2.addData(1, 2, 3, 4)
      registerPersistedView("a", query = dfFlowFunc(ints1.toDF()))
      registerPersistedView("b", query = dfFlowFunc(ints2.toDF()))
      registerPersistedView(
        "c",
        query = FlowAnalysis.createFlowFunctionFromLogicalPlan(
          Union(
            Seq(
              UnresolvedRelation(
                TableIdentifier("a"),
                extraOptions = CaseInsensitiveStringMap.empty(),
                isStreaming = true
              ),
              UnresolvedRelation(
                TableIdentifier("b"),
                extraOptions = CaseInsensitiveStringMap.empty(),
                isStreaming = true
              )
            )
          )
        )
      )
    }

    val p = new P().resolveToDataflowGraph()
    verifyFlowSchema(
      p,
      fullyQualifiedIdentifier("c"),
      new StructType().add("value", IntegerType, false)
    )
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("c")).inputs == Set(
        fullyQualifiedIdentifier("a"),
        fullyQualifiedIdentifier("b")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("MultipleInputs") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerPersistedView("b", query = dfFlowFunc(Seq(4, 5, 6).toDF("y")))
      registerPersistedView(
        "c",
        query = sqlFlowFunc(spark, "SELECT x AS z FROM a UNION SELECT y AS z FROM b")
      )
    }
    val p = new P().resolveToDataflowGraph()
    val schema = new StructType().add("z", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("c"), schema)
  }

  test("Connect retains and fuses confs") {
    val session = spark
    import session.implicits._

    // a -> b \
    //          d
    //      c /
    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1).toDF("x")), Map("a" -> "a-val"))
      registerPersistedView("b", query = readFlowFunc("a"), Map("b" -> "b-val"))
      registerPersistedView("c", query = dfFlowFunc(Seq(2).toDF("x")), Map("c" -> "c-val"))
      registerTable(
        "d",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM b UNION SELECT * FROM c")),
        Map("d" -> "d-val")
      )
    }
    val graph = p.resolveToDataflowGraph()
    assert(
      graph
        .flow(fullyQualifiedIdentifier("d"))
        .sqlConf == Map("a" -> "a-val", "b" -> "b-val", "c" -> "c-val", "d" -> "d-val")
    )
  }

  test("Confs aren't fused past materialization points") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1).toDF("x")), Map("a" -> "a-val"))
      registerTable("b", query = Option(readFlowFunc("a")), Map("b" -> "b-val"))
      registerPersistedView(
        "c",
        query = dfFlowFunc(Seq(2).toDF("x")),
        sqlConf = Map("c" -> "c-val")
      )
      registerTable(
        "d",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM b UNION SELECT * FROM c")),
        Map("d" -> "d-val")
      )
    }
    val graph = p.resolveToDataflowGraph()
    assert(graph.flow(fullyQualifiedIdentifier("a")).sqlConf == Map("a" -> "a-val"))
    assert(
      graph
        .flow(fullyQualifiedIdentifier("b"))
        .sqlConf == Map("a" -> "a-val", "b" -> "b-val")
    )
    assert(graph.flow(fullyQualifiedIdentifier("c")).sqlConf == Map("c" -> "c-val"))
    assert(
      graph
        .flow(fullyQualifiedIdentifier("d"))
        .sqlConf == Map("c" -> "c-val", "d" -> "d-val")
    )
  }

  test("Setting the same conf with the same value is totally cool") {
    val session = spark
    import session.implicits._

    val p = new TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")), Map("key" -> "val"))
      registerPersistedView("b", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")), Map("key" -> "val"))
      registerTable(
        "c",
        query = Option(sqlFlowFunc(spark, "SELECT * FROM a UNION SELECT * FROM b")),
        Map("key" -> "val")
      )
    }
    val graph = p.resolveToDataflowGraph()
    assert(graph.flow(fullyQualifiedIdentifier("c")).sqlConf == Map("key" -> "val"))
  }

  test("Named query only") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      registerPersistedView("a", query = dfFlowFunc(Seq(1, 2, 3).toDF("x")))
      registerTable("b")
      registerFlow("b", "`b-query`", readFlowFunc("a"))
    }
    val p = new P().resolveToDataflowGraph()
    val schema = new StructType().add("x", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("a"), schema)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b-query"), schema)
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("b-query")).inputs == Set(
        fullyQualifiedIdentifier("a")
      ),
      "Flow did not have the expected inputs"
    )
  }

  test("Default query and named query") {
    val session = spark
    import session.implicits._

    class P extends TestGraphRegistrationContext(spark) {
      val mem = MemoryStream[Int]
      registerPersistedView("a", query = dfFlowFunc(mem.toDF()))
      registerTable("b")
      registerFlow("b", "b", dfFlowFunc(mem.toDF().select($"value" as "y")))
      registerFlow("b", "b2", readStreamFlowFunc("a"))
    }
    val p = new P().resolveToDataflowGraph()
    val schema = new StructType().add("value", IntegerType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("a"), schema)
    verifyFlowSchema(p, fullyQualifiedIdentifier("b2"), schema)
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("b2")).inputs == Set(
        fullyQualifiedIdentifier("a")
      ),
      "Flow did not have the expected inputs"
    )
    verifyFlowSchema(
      p,
      fullyQualifiedIdentifier("b"),
      new StructType().add("y", IntegerType, false)
    )
    assert(
      p.resolvedFlow(fullyQualifiedIdentifier("b")).inputs == Set.empty,
      "Flow did not have the expected inputs"
    )
  }

  test("Multi-query table with 2 complete queries") {
    class P extends TestGraphRegistrationContext(spark) {
      registerTable("a")
      registerFlow("a", "a", query = dfFlowFunc(spark.range(5).toDF()))
      registerFlow("a", "a2", query = dfFlowFunc(spark.range(6).toDF()))
    }
    val p = new P().resolveToDataflowGraph()
    val schema = new StructType().add("id", LongType, false)
    verifyFlowSchema(p, fullyQualifiedIdentifier("a"), schema)
  }

  test("Correct types of flows after connection") {
    val session = spark
    import session.implicits._

    val graph = new TestGraphRegistrationContext(spark) {
      val mem = MemoryStream[Int]
      mem.addData(1, 2)
      registerPersistedView("complete-view", query = dfFlowFunc(Seq(1, 2).toDF("x")))
      registerPersistedView("incremental-view", query = dfFlowFunc(mem.toDF()))
      registerTable("`complete-table`", query = Option(readFlowFunc("complete-view")))
      registerTable("`incremental-table`")
      registerFlow(
        "`incremental-table`",
        "`incremental-table`",
        FlowAnalysis.createFlowFunctionFromLogicalPlan(
          UnresolvedRelation(
            TableIdentifier("incremental-view"),
            extraOptions = CaseInsensitiveStringMap.empty(),
            isStreaming = true
          )
        )
      )
      registerFlow(
        "`incremental-table`",
        "`append-once`",
        dfFlowFunc(Seq(1, 2).toDF("x")),
        once = true
      )
    }.resolveToDataflowGraph()

    assert(
      graph
        .flow(fullyQualifiedIdentifier("complete-view"))
        .isInstanceOf[CompleteFlow]
    )
    assert(
      graph
        .flow(fullyQualifiedIdentifier("incremental-view"))
        .isInstanceOf[StreamingFlow]
    )
    assert(
      graph
        .flow(fullyQualifiedIdentifier("complete-table"))
        .isInstanceOf[CompleteFlow]
    )
    assert(
      graph
        .flow(fullyQualifiedIdentifier("incremental-table"))
        .isInstanceOf[StreamingFlow]
    )
    assert(
      graph
        .flow(fullyQualifiedIdentifier("append-once"))
        .isInstanceOf[AppendOnceFlow]
    )
  }

  test("Pipeline level default spark confs are applied with correct precedence") {
    val session = spark
    import session.implicits._

    val P = new TestGraphRegistrationContext(
      spark,
      Map("default.conf" -> "value")
    ) {
      registerTable(
        "a",
        query = Option(dfFlowFunc(Seq(1, 2, 3).toDF("x"))),
        sqlConf = Map("other.conf" -> "value")
      )
      registerTable(
        "b",
        query = Option(sqlFlowFunc(spark, "SELECT x as y FROM a")),
        sqlConf = Map("default.conf" -> "other-value")
      )
    }
    val p = P.resolveToDataflowGraph()

    assert(
      p.flow(fullyQualifiedIdentifier("a")).sqlConf == Map(
        "default.conf" -> "value",
        "other.conf" -> "value"
      )
    )

    assert(
      p.flow(fullyQualifiedIdentifier("b")).sqlConf == Map(
        "default.conf" -> "other-value"
      )
    )
  }

  /** Verifies the [[DataflowGraph]] has the specified [[Flow]] with the specified schema. */
  private def verifyFlowSchema(
      pipeline: DataflowGraph,
      identifier: TableIdentifier,
      expected: StructType): Unit = {
    assert(
      pipeline.flow.contains(identifier),
      s"Flow ${identifier.unquotedString} not found," +
        s" all flow names: ${pipeline.flow.keys.map(_.unquotedString)}"
    )
    assert(
      pipeline.resolvedFlow.contains(identifier),
      s"Flow ${identifier.unquotedString} has not been resolved"
    )
    assert(
      pipeline.resolvedFlow(identifier).schema == expected,
      s"Flow ${identifier.unquotedString} has the wrong schema"
    )
  }
}
