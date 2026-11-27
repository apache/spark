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

package org.apache.spark.graph

import org.apache.logging.log4j.Level

import org.apache.spark.SparkRuntimeException
import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.graph.GraphXAdapter.Attr
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.graphx.lib.{ConnectedComponents, LabelPropagation, PageRank, ShortestPaths, StronglyConnectedComponents, SVDPlusPlus, TriangleCount}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, Encoders, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.graph.ParquetCheckpointer
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType}

class GraphXAdapterSuite extends QueryTest with GraphFrameTestBase {
  import testImplicits._

  private def withCheckpointer[T](body: ParquetCheckpointer => T): T = {
    val checkpointer = ParquetCheckpointer(spark, "test")
    try body(checkpointer)
    finally checkpointer.close()
  }

  // Two components: the cycle a -> b -> c -> a and the edge d -> e; f is isolated.
  private val names = Seq("a", "b", "c", "d", "e", "f")
  private val links = Seq(("a", "b", 1.0), ("b", "c", 2.0), ("c", "a", 3.0), ("d", "e", 4.0))

  private def stringGraph: GraphFrame = GraphFrame(
    names.zipWithIndex.toDF(ID, "x"),
    links.toDF(SRC, DST, "w"))

  // The same graph with the IDs 1 to 6.
  private def integralGraph: GraphFrame = {
    val g = stringGraph
    val ids = names.zipWithIndex.map { case (n, i) => n -> (i + 1L) }.toDF("name", "num")
    GraphFrame(
      g.vertices.join(ids, col(ID) === col("name")).select(col("num").as(ID), col("x")),
      g.edges
        .join(ids.select(col("name").as(SRC), col("num").as("s")), SRC)
        .join(ids.select(col("name").as(DST), col("num").as("d")), DST)
        .select(col("s").as(SRC), col("d").as(DST), col("w")))
  }

  private def graphs: Seq[(String, GraphFrame)] =
    Seq("string" -> stringGraph, "integral" -> integralGraph)

  // The result keyed by the name of the vertex, whatever the ID type.
  private def byName(g: GraphFrame, result: DataFrame): Map[String, Any] = {
    val rows = result.collect()
    if (g.vertices.schema(ID).dataType == StringType) {
      rows.map(r => r.getString(0) -> r.get(1)).toMap
    } else {
      rows.map(r => names(r.getLong(0).toInt - 1) -> r.get(1)).toMap
    }
  }

  private def outDegrees(gx: Graph[Unit, Unit]): RDD[(VertexId, Int)] =
    gx.vertices.leftJoin(gx.outDegrees)((_, _, d) => d.getOrElse(0))

  test("the result is keyed by the original IDs") {
    graphs.foreach { case (kind, g) =>
      withCheckpointer { checkpointer =>
        val result = GraphXAdapter.run(
          g, checkpointer, Attr.unit, Attr.unit, "degree", Encoders.scalaInt) { gx =>
          outDegrees(gx)
        }
        assert(result.columns.toSeq == Seq(ID, "degree"), kind)
        checkColumnType(result.schema, ID, g.vertices.schema(ID).dataType)
        assert(
          byName(g, result) ==
            Map("a" -> 1, "b" -> 1, "c" -> 1, "d" -> 1, "e" -> 0, "f" -> 0),
          kind)
      }
    }
  }

  test("integral ID types are kept") {
    Seq(ByteType, ShortType, IntegerType, LongType).foreach { idType =>
      val g = integralGraph
      val typed = GraphFrame(
        g.vertices.select(col(ID).cast(idType).as(ID)),
        g.edges.select(col(SRC).cast(idType).as(SRC), col(DST).cast(idType).as(DST)))
      withCheckpointer { checkpointer =>
        val result = GraphXAdapter.run(
          typed, checkpointer, Attr.unit, Attr.unit, "degree", Encoders.scalaInt) { gx =>
          outDegrees(gx)
        }
        checkColumnType(result.schema, ID, idType)
        assert(result.count() == names.size)
      }
    }
  }

  test("only the requested columns are passed to GraphX") {
    graphs.foreach { case (kind, g0) =>
      val g = GraphFrame(
        g0.vertices.withColumnRenamed("x", "x.y").withColumn("unused", col(ID)),
        g0.edges.withColumn("unused", col(SRC)))
      withCheckpointer { checkpointer =>
        val vertices = Attr(Seq("x.y"), _.getInt(0))
        val edges = Attr(Seq("w"), r => (r.length, r.getDouble(0)))
        val result = GraphXAdapter.run(
          g, checkpointer, vertices, edges, "sum", Encoders.scalaDouble) { gx =>
          // The weight of the incoming edges plus the attribute of their sources.
          gx.aggregateMessages[Double](
            ctx => {
              assert(ctx.attr._1 == 1)
              ctx.sendToDst(ctx.attr._2 + ctx.srcAttr)
            },
            _ + _)
        }
        // x is the position of the name: a = 0, b = 1, c = 2, d = 3.
        assert(
          byName(g, result) == Map("a" -> 5.0, "b" -> 1.0, "c" -> 3.0, "e" -> 7.0),
          kind)
      }
    }
  }

  test("a struct result becomes a struct column") {
    withCheckpointer { checkpointer =>
      val result = GraphXAdapter.run(
        stringGraph,
        checkpointer,
        Attr.unit,
        Attr.unit,
        "r",
        Encoders.tuple(Encoders.scalaInt, Encoders.STRING)) { gx =>
        outDegrees(gx).mapValues(d => (d, d.toString))
      }
      checkAnswer(result.where(col(ID) === "a").select("r._1", "r._2"), Row(1, "1"))
    }
  }

  test("the result column must not be the ID column") {
    withCheckpointer { checkpointer =>
      checkError(
        exception = intercept[AnalysisException] {
          GraphXAdapter.run(
            stringGraph, checkpointer, Attr.unit, Attr.unit, "ID", Encoders.scalaInt) {
            gx => outDegrees(gx)
          }
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`ID`"))
    }
  }

  test("null IDs") {
    val nullVertex = GraphFrame(
      Seq(Some(1L), None).toDF(ID),
      Seq.empty[(Long, Long)].toDF(SRC, DST))
    val nullEdge = GraphFrame(
      Seq(1L, 2L).toDF(ID),
      Seq((Some(1L), Some(2L)), (Some(1L), None)).toDF(SRC, DST))
    Seq(
      nullVertex -> Map("dataFrameKind" -> "vertices", "columns" -> s"`$ID`"),
      nullEdge -> Map("dataFrameKind" -> "edges", "columns" -> s"`$SRC`, `$DST`")).foreach {
      case (g, parameters) =>
        assertNoPersistedDataLeak {
          withCheckpointer { checkpointer =>
            checkError(
              exception = intercept[SparkRuntimeException] {
                GraphXAdapter.run(
                  g, checkpointer, Attr.unit, Attr.unit, "r", Encoders.scalaInt) {
                  gx => outDegrees(gx)
                }.collect()
              },
              condition = "GRAPH_INVALID.NULL_IDS",
              parameters = parameters)
          }
        }
    }
  }

  // Each algorithm of graphx.lib, with a result that the suite can encode.
  private val algorithms: Seq[(String, GraphFrame => ParquetCheckpointer => DataFrame)] = {
    val weight = Attr(Seq("w"), _.getDouble(0))
    def topology[T](encoder: Encoder[T])(f: Graph[Unit, Unit] => RDD[(VertexId, T)]) =
      (g: GraphFrame) =>
        (checkpointer: ParquetCheckpointer) =>
          GraphXAdapter.run(g, checkpointer, Attr.unit, Attr.unit, "r", encoder)(f)
    // The sources of the algorithms that need them, taken from the GraphX graph itself.
    def sources(gx: Graph[Unit, Unit]): Seq[VertexId] =
      gx.vertices.keys.collect().sorted.take(2).toSeq
    Seq(
      "connected components" -> topology(Encoders.scalaLong) { gx =>
        ConnectedComponents.run(gx).vertices
      },
      "connected components with max iterations" -> topology(Encoders.scalaLong) { gx =>
        ConnectedComponents.run(gx, 2).vertices
      },
      "strongly connected components" -> topology(Encoders.scalaLong) { gx =>
        StronglyConnectedComponents.run(gx, 5).vertices
      },
      "label propagation" -> topology(Encoders.scalaLong) { gx =>
        LabelPropagation.run(gx, 5).vertices
      },
      "triangle count" -> topology(Encoders.scalaInt) { gx =>
        TriangleCount.run(gx).vertices
      },
      "page rank" -> topology(Encoders.scalaDouble) { gx =>
        PageRank.run(gx, 5).vertices
      },
      "page rank until convergence" -> topology(Encoders.scalaDouble) { gx =>
        PageRank.runUntilConvergence(gx, 0.01).vertices
      },
      "personalized page rank" -> topology(Encoders.scalaDouble) { gx =>
        PageRank.runWithOptions(gx, 5, srcId = Some(sources(gx).head)).vertices
      },
      "parallel personalized page rank" -> topology(implicitly[Encoder[Array[Double]]]) { gx =>
        PageRank
          .runParallelPersonalizedPageRank(gx, 5, 0.15, sources(gx).toArray)
          .vertices
          .mapValues(_.toArray)
      },
      "shortest paths" -> topology(implicitly[Encoder[Seq[Int]]]) { gx =>
        val landmarks = sources(gx)
        ShortestPaths
          .run(gx, landmarks)
          .vertices
          .mapValues(m => landmarks.map(l => m.getOrElse(l, -1)))
      },
      "svd++" -> { (g: GraphFrame) => (checkpointer: ParquetCheckpointer) =>
        GraphXAdapter.run(g, checkpointer, Attr.unit, weight, "r", Encoders.scalaDouble) { gx =>
          val conf = new SVDPlusPlus.Conf(2, 3, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015)
          SVDPlusPlus.run(gx.edges, conf)._1.vertices.mapValues(_._3)
        }
      })
  }

  algorithms.foreach { case (name, algorithm) =>
    test(s"no cached RDDs are left: $name") {
      graphs.foreach { case (kind, g) =>
        assertNoPersistedDataLeak {
          withCheckpointer { checkpointer =>
            val result = algorithm(g)(checkpointer)
            // SVD++ builds its graph from the edges, so it has no isolated vertices.
            val expected = if (name == "svd++") names.size - 1 else names.size
            assert(result.count() == expected, kind)
          }
        }
      }
    }
  }

  // graphframes#360: a streaming query that runs an algorithm in every micro-batch.
  test("repeated runs do not accumulate cached RDDs") {
    val sc = spark.sparkContext
    val g = stringGraph
    assertNoPersistedDataLeak {
      val before = sc.getPersistentRDDs.size
      (1 to 30).foreach { batch =>
        withCheckpointer { checkpointer =>
          val result = GraphXAdapter.run(
            g, checkpointer, Attr.unit, Attr.unit, "label", Encoders.scalaLong) { gx =>
            LabelPropagation.run(gx, 5).vertices
          }
          assert(result.count() == names.size)
        }
        assert(sc.getPersistentRDDs.size == before, s"batch $batch")
      }
    }
  }

  test("cached data of others is kept") {
    val sc = spark.sparkContext
    def otherGraph(): Graph[Int, Int] =
      Graph.fromEdges(sc.parallelize(Seq(Edge(1L, 2L, 1))), 0).cache()
    val before = otherGraph()
    before.vertices.count()
    val cachedDf = spark.range(10).toDF().cache()
    cachedDf.count()
    var during: Graph[Int, Int] = null
    try {
      val beforeIds = sc.getPersistentRDDs.keySet
      withCheckpointer { checkpointer =>
        GraphXAdapter.run(
          stringGraph, checkpointer, Attr.unit, Attr.unit, "label", Encoders.scalaLong) {
          gx =>
            // Created during the run, but not from the graph of the run.
            during = otherGraph()
            during.vertices.count()
            LabelPropagation.run(gx, 5).vertices
        }.collect()
      }
      assert(beforeIds.subsetOf(sc.getPersistentRDDs.keySet))
      Seq(before, during).foreach { other =>
        assert(other.vertices.getStorageLevel.useMemory)
        assert(other.edges.getStorageLevel.useMemory)
      }
      assert(cachedDf.storageLevel.useMemory)
      checkAnswer(cachedDf, (0 until 10).map(i => Row(i.toLong)))
    } finally {
      before.unpersist()
      Option(during).foreach(_.unpersist())
      cachedDf.unpersist()
    }
  }

  test("a failed algorithm leaves no cached RDDs") {
    assertNoPersistedDataLeak {
      withCheckpointer { checkpointer =>
        val e = intercept[IllegalStateException] {
          GraphXAdapter.run(
            stringGraph, checkpointer, Attr.unit, Attr.unit, "r", Encoders.scalaInt) {
            gx =>
              gx.mapVertices((_, _) => 1).cache().vertices.count()
              throw new IllegalStateException("boom")
          }
        }
        assert(e.getMessage == "boom")
      }
    }
  }

  test("Pregel checkpoints are reported") {
    val conf = spark.sparkContext.conf
    val key = "spark.graphx.pregel.checkpointInterval"
    def runOnce(): Unit = withCheckpointer { checkpointer =>
      GraphXAdapter.run(
        stringGraph, checkpointer, Attr.unit, Attr.unit, "degree", Encoders.scalaInt) {
        gx => outDegrees(gx)
      }.collect()
    }
    def warnings(): Seq[String] = {
      val appender = new LogAppender("pregel checkpoints")
      withLogAppender(appender, level = Some(Level.WARN)) {
        runOnce()
      }
      appender.loggingEvents.map(_.getMessage.getFormattedMessage).filter(_.contains(key)).toSeq
    }
    assert(warnings().isEmpty)
    conf.set(key, "2")
    try {
      val logged = warnings()
      assert(logged.size == 1)
      assert(logged.head.contains(s"$key is 2"))
    } finally {
      conf.remove(key)
    }
  }
}
