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

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class GraphFrameSuite extends QueryTest with GraphFrameTestBase {

  private def vertexData: DataFrame = spark
    .createDataFrame(Seq((0L, "a", "f"), (1L, "b", "m"), (2L, "c", "m"), (3L, "d", "f")))
    .toDF(ID, "name", "gender")

  private def edgeData: DataFrame = spark
    .createDataFrame(
      Seq(
        (0L, 1L, "friend"),
        (1L, 0L, "follow"),
        (1L, 2L, "friend"),
        (2L, 3L, "follow"),
        (2L, 0L, "unknown")))
    .toDF(SRC, DST, "relationship")

  private def graph: GraphFrame = GraphFrame(vertexData, edgeData)

  private def emptyGraph: GraphFrame = GraphFrame(vertexData.limit(0), edgeData.limit(0))

  test("construction from DataFrames") {
    val g = graph
    checkSchemaInvariant(g)
    checkColumnType(g.vertices.schema, ID, LongType)
    checkColumnType(g.edges.schema, SRC, LongType)
    checkColumnType(g.edges.schema, DST, LongType)
    checkAnswer(g.vertices, vertexData)
    checkAnswer(g.edges, edgeData)
  }

  test("construction with dots and backquotes in column names") {
    Seq("a.name" -> "the.relationship", "a `name`" -> "the `relationship`").foreach {
      case (vertexColumn, edgeColumn) =>
        val g = GraphFrame(
          vertexData.withColumnRenamed("name", vertexColumn),
          edgeData.withColumnRenamed("relationship", edgeColumn))
        checkSchemaInvariant(g)
        assert(g.vertexColumns.contains(vertexColumn))
        assert(g.edgeColumns.contains(edgeColumn))
        checkAnswer(g.vertices, vertexData)
        checkAnswer(g.edges, edgeData)
        assert(g.toString.startsWith("GraphFrame(v:"))
        checkAnswer(g.filterVertices(col(ID) > 0).vertices.select(ID), Seq(1L, 2L, 3L).map(Row(_)))
    }
  }

  test("construction fails when required columns are missing") {
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData.withColumnRenamed(ID, "uid"), edgeData)
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`id`", "proposal" -> "`uid`, `name`, `gender`"))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.withColumnRenamed(SRC, "srcId"))
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`src`", "proposal" -> "`srcId`, `dst`, `relationship`"))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.withColumnRenamed(DST, "dstId"))
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`dst`", "proposal" -> "`src`, `dstId`, `relationship`"))
    checkError(
      exception = intercept[AnalysisException](GraphFrame(spark.emptyDataFrame, edgeData)),
      condition = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`id`"))
  }

  test("construction fails on null DataFrames") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](GraphFrame(null, edgeData)),
      condition = "GRAPH_NULL_DATAFRAME",
      sqlState = "22004",
      parameters = Map("dataFrameKind" -> "vertices"))
    checkError(
      exception = intercept[SparkIllegalArgumentException](GraphFrame(vertexData, null)),
      condition = "GRAPH_NULL_DATAFRAME",
      sqlState = "22004",
      parameters = Map("dataFrameKind" -> "edges"))
    checkError(
      exception = intercept[SparkIllegalArgumentException](GraphFrame(null, null)),
      condition = "GRAPH_NULL_DATAFRAME",
      sqlState = "22004",
      parameters = Map("dataFrameKind" -> "vertices"))
  }

  test("construction fails when required columns are duplicated") {
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData.select(col(ID), col(ID), col("name")), edgeData)
      },
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      sqlState = "42702",
      parameters = Map("name" -> "`id`", "n" -> "2"))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.select(col(SRC), col(DST), col(SRC)))
      },
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      sqlState = "42702",
      parameters = Map("name" -> "`src`", "n" -> "2"))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.select(col(SRC), col(DST), col(DST)))
      },
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      sqlState = "42702",
      parameters = Map("name" -> "`dst`", "n" -> "2"))
  }

  test("required columns differing only in case follow spark.sql.caseSensitive") {
    val vertices = vertexData.select(col(ID), col(ID).as("ID"), col("name"))
    val edges = edgeData.select(col(SRC), col(DST), col(DST).as("DST"))
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkError(
        exception = intercept[AnalysisException](GraphFrame(vertices, edgeData)),
        condition = "AMBIGUOUS_COLUMN_OR_FIELD",
        sqlState = "42702",
        parameters = Map("name" -> "`id`", "n" -> "2"))
      checkError(
        exception = intercept[AnalysisException](GraphFrame(vertexData, edges)),
        condition = "AMBIGUOUS_COLUMN_OR_FIELD",
        sqlState = "42702",
        parameters = Map("name" -> "`dst`", "n" -> "2"))
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val g = GraphFrame(vertices, edges)
      checkAnswer(g.vertices.select(ID), vertexData.select(ID))
      checkAnswer(g.edges.select(SRC, DST), edgeData.select(SRC, DST))
    }
  }

  test("construction fails when id, src and dst types differ") {
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData.withColumn(ID, col(ID).cast(IntegerType)), edgeData)
      },
      condition = "GRAPH_ID_TYPE_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "idColumn" -> "`id`",
        "srcColumn" -> "`src`",
        "dstColumn" -> "`dst`",
        "idType" -> "\"INT\"",
        "srcType" -> "\"BIGINT\"",
        "dstType" -> "\"BIGINT\""))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.withColumn(DST, col(DST).cast(StringType)))
      },
      condition = "GRAPH_ID_TYPE_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "idColumn" -> "`id`",
        "srcColumn" -> "`src`",
        "dstColumn" -> "`dst`",
        "idType" -> "\"BIGINT\"",
        "srcType" -> "\"BIGINT\"",
        "dstType" -> "\"STRING\""))
  }

  test("all supported id types") {
    Seq(ByteType, ShortType, IntegerType, LongType, StringType).foreach { dataType =>
      val g = GraphFrame(
        vertexData.withColumn(ID, col(ID).cast(dataType)),
        edgeData
          .withColumn(SRC, col(SRC).cast(dataType))
          .withColumn(DST, col(DST).cast(dataType)))
      checkColumnType(g.vertices.schema, ID, dataType)
      checkColumnType(g.edges.schema, SRC, dataType)
      checkColumnType(g.edges.schema, DST, dataType)
    }
  }

  test("construction fails on unsupported id types") {
    val supportedTypes = "\"TINYINT\", \"SMALLINT\", \"INT\", \"BIGINT\", \"STRING\""
    Seq("DOUBLE", "DECIMAL(20,0)", "STRING COLLATE UTF8_LCASE").foreach { sqlType =>
      checkError(
        exception = intercept[AnalysisException] {
          GraphFrame(
            vertexData.withColumn(ID, col(ID).cast(sqlType)),
            edgeData
              .withColumn(SRC, col(SRC).cast(sqlType))
              .withColumn(DST, col(DST).cast(sqlType)))
        },
        condition = "GRAPH_UNSUPPORTED_ID_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "dataFrameKind" -> "vertices",
          "columnName" -> "`id`",
          "columnType" -> ("\"" + sqlType + "\""),
          "supportedTypes" -> supportedTypes))
    }
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(vertexData, edgeData.withColumn(SRC, col(SRC).cast("DOUBLE")))
      },
      condition = "GRAPH_UNSUPPORTED_ID_TYPE",
      sqlState = "42K09",
      parameters = Map(
        "dataFrameKind" -> "edges",
        "columnName" -> "`src`",
        "columnType" -> "\"DOUBLE\"",
        "supportedTypes" -> supportedTypes))
    checkError(
      exception = intercept[AnalysisException] {
        GraphFrame(
          vertexData.withColumn(ID, array(col(ID))),
          edgeData
            .withColumn(SRC, array(col(SRC)))
            .withColumn(DST, array(col(DST))))
      },
      condition = "GRAPH_UNSUPPORTED_ID_TYPE",
      sqlState = "42K09",
      parameters = Map(
        "dataFrameKind" -> "vertices",
        "columnName" -> "`id`",
        "columnType" -> "\"ARRAY<BIGINT>\"",
        "supportedTypes" -> supportedTypes))
  }

  test("validate") {
    graph.validate()
    graph.validate(checkVertices = false, intermediateStorageLevel = StorageLevel.MEMORY_ONLY)
    emptyGraph.validate()
  }

  test("validate fails on duplicate vertices") {
    val g = GraphFrame(vertexData.union(vertexData.where(col(ID) === 1L)), edgeData)
    Seq(true, false).foreach { checkVertices =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          g.validate(checkVertices, StorageLevel.MEMORY_AND_DISK)
        },
        condition = "GRAPH_INVALID.DUPLICATE_VERTICES",
        sqlState = "23000",
        parameters = Map("count" -> "1", "idColumn" -> "`id`"))
    }
  }

  test("validate fails on NULL ids") {
    val nullVertex = spark.sql("SELECT CAST(NULL AS BIGINT) AS id, 'x' AS name, 'f' AS gender")
    val withNullVertex = GraphFrame(vertexData.union(nullVertex), edgeData)
    val withNullSrc = GraphFrame(
      vertexData,
      edgeData.union(spark.sql("SELECT CAST(NULL AS BIGINT), 1L, 'new'")))
    val withNullDst = GraphFrame(
      vertexData,
      edgeData.union(spark.sql("SELECT 1L, CAST(NULL AS BIGINT), 'new'")))
    Seq(true, false).foreach { checkVertices =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          withNullVertex.validate(checkVertices, StorageLevel.MEMORY_AND_DISK)
        },
        condition = "GRAPH_INVALID.NULL_IDS",
        sqlState = "23000",
        parameters = Map("dataFrameKind" -> "vertices", "columns" -> "`id`"))
      Seq(withNullSrc, withNullDst).foreach { g =>
        checkError(
          exception = intercept[SparkRuntimeException] {
            g.validate(checkVertices, StorageLevel.MEMORY_AND_DISK)
          },
          condition = "GRAPH_INVALID.NULL_IDS",
          sqlState = "23000",
          parameters = Map("dataFrameKind" -> "edges", "columns" -> "`src`, `dst`"))
      }
    }
  }

  test("validate reports NULL ids before duplicate vertices") {
    val nullVertex = spark.sql("SELECT CAST(NULL AS BIGINT) AS id, 'x' AS name, 'f' AS gender")
    val g = GraphFrame(vertexData.union(vertexData).union(nullVertex), edgeData)
    checkError(
      exception = intercept[SparkRuntimeException](g.validate()),
      condition = "GRAPH_INVALID.NULL_IDS",
      sqlState = "23000",
      parameters = Map("dataFrameKind" -> "vertices", "columns" -> "`id`"))
  }

  test("validate fails when edges reference more vertices than exist") {
    val g = GraphFrame(vertexData, edgeData.union(edgeData.selectExpr("src", "4L", "'new'")))
    checkError(
      exception = intercept[SparkRuntimeException] {
        g.validate()
      },
      condition = "GRAPH_INVALID.EDGE_VERTICES_COUNT_MISMATCH",
      sqlState = "23000",
      parameters = Map("edgeVertexCount" -> "5", "vertexCount" -> "4"))
    g.validate(checkVertices = false, intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK)
  }

  test("validate fails when edges reference vertices not in the vertices DataFrame") {
    val g = GraphFrame(
      vertexData,
      edgeData.where(col(SRC) =!= 3L && col(DST) =!= 3L).union(
        spark.createDataFrame(Seq((0L, 4L, "new"))).toDF(SRC, DST, "relationship")))
    checkError(
      exception = intercept[SparkRuntimeException] {
        g.validate()
      },
      condition = "GRAPH_INVALID.EDGE_VERTEX_NOT_IN_VERTICES",
      sqlState = "23000",
      parameters = Map("count" -> "1", "srcColumn" -> "`src`", "dstColumn" -> "`dst`"))
  }

  test("assertNoPersistedDataLeak detects leaked Datasets and RDDs") {
    val leakedDataset = vertexData.select(ID)
    intercept[TestFailedException] {
      assertNoPersistedDataLeak {
        leakedDataset.persist()
      }
    }
    leakedDataset.unpersist(blocking = true)

    val leakedRdd = spark.sparkContext.parallelize(1 to 10)
    intercept[TestFailedException] {
      assertNoPersistedDataLeak {
        leakedRdd.persist().count()
      }
    }
    leakedRdd.unpersist(blocking = true)

    assertNoPersistedDataLeak {
      val released = vertexData.select(ID).persist()
      released.count()
      released.unpersist()
    }
  }

  test("validate releases all persisted data on every exit path") {
    val nullVertex = spark.sql("SELECT CAST(NULL AS BIGINT) AS id, 'x' AS name, 'f' AS gender")
    val graphs = Seq(
      graph,
      GraphFrame(vertexData.union(nullVertex), edgeData),
      GraphFrame(vertexData, edgeData.union(spark.sql("SELECT 1L, CAST(NULL AS BIGINT), 'x'"))),
      GraphFrame(vertexData.union(vertexData.where(col(ID) === 1L)), edgeData),
      GraphFrame(vertexData, edgeData.union(edgeData.selectExpr("src", "4L", "'new'"))),
      GraphFrame(
        vertexData,
        edgeData.where(col(SRC) =!= 3L && col(DST) =!= 3L).union(
          spark.createDataFrame(Seq((0L, 4L, "new"))).toDF(SRC, DST, "relationship"))))
    for {
      g <- graphs
      checkVertices <- Seq(true, false)
    } {
      assertNoPersistedDataLeak {
        try {
          g.validate(checkVertices, StorageLevel.MEMORY_AND_DISK)
        } catch {
          case _: SparkRuntimeException =>
        }
      }
    }
  }

  test("validate does not change the storage level of the input DataFrames") {
    val g = graph
    g.validate()
    assert(g.vertices.storageLevel == StorageLevel.NONE)
    assert(g.edges.storageLevel == StorageLevel.NONE)
  }

  test("vertex and edge columns") {
    val g = graph
    assert(g.vertexColumns.toSeq == Seq(ID, "name", "gender"))
    assert(g.edgeColumns.toSeq == Seq(SRC, DST, "relationship"))
  }

  test("degree metrics") {
    val g = graph
    assert(g.outDegrees.columns.toSeq == Seq(ID, "outDegree"))
    checkColumnType(g.outDegrees.schema, "outDegree", IntegerType)
    checkAnswer(g.outDegrees, Seq(Row(0L, 1), Row(1L, 2), Row(2L, 2)))

    assert(g.inDegrees.columns.toSeq == Seq(ID, "inDegree"))
    checkColumnType(g.inDegrees.schema, "inDegree", IntegerType)
    checkAnswer(g.inDegrees, Seq(Row(0L, 2), Row(1L, 1), Row(2L, 1), Row(3L, 1)))

    assert(g.degrees.columns.toSeq == Seq(ID, "degree"))
    checkColumnType(g.degrees.schema, "degree", IntegerType)
    checkAnswer(g.degrees, Seq(Row(0L, 3), Row(1L, 3), Row(2L, 3), Row(3L, 1)))
  }

  test("degree metrics skip isolated vertices and count self-loops twice") {
    val g = GraphFrame(
      vertexData,
      spark.createDataFrame(Seq((0L, 0L, "self"), (0L, 1L, "friend"))).toDF(SRC, DST, "rel"))
    checkAnswer(g.outDegrees, Seq(Row(0L, 2)))
    checkAnswer(g.inDegrees, Seq(Row(0L, 1), Row(1L, 1)))
    checkAnswer(g.degrees, Seq(Row(0L, 3), Row(1L, 1)))
  }

  test("type degree metrics") {
    val g = graph
    val types = Seq("follow", "friend", "unknown")
    val degreesType = StructType(types.map(StructField(_, IntegerType, nullable = false)))

    val out = g.typeOutDegree("relationship")
    assert(out.schema == StructType(Seq(
      StructField(ID, LongType, nullable = false),
      StructField("outDegrees", degreesType, nullable = false))))
    checkAnswer(out, Seq(Row(0L, Row(0, 1, 0)), Row(1L, Row(1, 1, 0)), Row(2L, Row(1, 0, 1))))

    val in = g.typeInDegree("relationship")
    assert(in.columns.toSeq == Seq(ID, "inDegrees"))
    checkAnswer(
      in,
      Seq(Row(0L, Row(1, 0, 1)), Row(1L, Row(0, 1, 0)), Row(2L, Row(0, 1, 0)),
        Row(3L, Row(1, 0, 0))))

    val all = g.typeDegree("relationship")
    assert(all.columns.toSeq == Seq(ID, "degrees"))
    checkAnswer(
      all,
      Seq(Row(0L, Row(1, 1, 1)), Row(1L, Row(1, 2, 0)), Row(2L, Row(1, 1, 1)),
        Row(3L, Row(1, 0, 0))))
  }

  test("type degree metrics with explicit edge types") {
    val g = graph
    // The struct fields follow the order of the given types, including the types without edges.
    val types = Seq("friend", "follow", "missing")
    Seq(
      g.typeOutDegree("relationship", types) ->
        Seq(Row(0L, Row(1, 0, 0)), Row(1L, Row(1, 1, 0)), Row(2L, Row(0, 1, 0))),
      g.typeInDegree("relationship", types) ->
        Seq(Row(0L, Row(0, 1, 0)), Row(1L, Row(1, 0, 0)), Row(2L, Row(1, 0, 0)),
          Row(3L, Row(0, 1, 0))),
      g.typeDegree("relationship", types) ->
        Seq(Row(0L, Row(1, 1, 0)), Row(1L, Row(2, 1, 0)), Row(2L, Row(1, 1, 0)),
          Row(3L, Row(0, 1, 0)))).foreach { case (df, expected) =>
      assert(df.schema(1).dataType.asInstanceOf[StructType].fieldNames.toSeq == types)
      checkAnswer(df, expected)
    }
    // Edges of the types that are not listed are not counted.
    checkAnswer(
      g.typeOutDegree("relationship", Seq("unknown")),
      Seq(Row(0L, Row(0)), Row(1L, Row(0)), Row(2L, Row(1))))
  }

  test("type degree metrics with edge types equal to id or with special characters") {
    val types = Seq(ID, "a.b", "a `b`")
    val g = GraphFrame(
      vertexData,
      spark.createDataFrame(Seq((0L, 1L, types(0)), (0L, 2L, types(1)), (1L, 2L, types(2))))
        .toDF(SRC, DST, "edge.type"))
    val out = g.typeOutDegree("`edge.type`", types)
    assert(out.columns.toSeq == Seq(ID, "outDegrees"))
    assert(out.schema("outDegrees").dataType.asInstanceOf[StructType].fieldNames.toSeq == types)
    checkAnswer(out, Seq(Row(0L, Row(1, 1, 0)), Row(1L, Row(0, 0, 1))))
    checkAnswer(
      g.typeDegree("`edge.type`").select(ID, "degrees.`id`", "degrees.`a.b`"),
      Seq(Row(0L, 1, 1), Row(1L, 1, 0), Row(2L, 0, 1)))
  }

  test("type degree metrics with integer edge types") {
    val g = GraphFrame(vertexData, edgeData.withColumn("w", col(SRC) % 2))
    val out = g.typeOutDegree("w")
    assert(out.schema("outDegrees").dataType.asInstanceOf[StructType].fieldNames.toSeq ==
      Seq("0", "1"))
    checkAnswer(out, Seq(Row(0L, Row(1, 0)), Row(1L, Row(0, 2)), Row(2L, Row(2, 0))))
  }

  test("type degree metrics fail on a missing edge type column") {
    checkError(
      exception = intercept[AnalysisException](graph.typeOutDegree("missing")),
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map(
        "objectName" -> "`missing`",
        "proposal" -> "`dst`, `src`, `relationship`"),
      context = ExpectedContext(fragment = "col", callSitePattern = ".*"))
  }

  test("filterEdges and dropIsolatedVertices") {
    val expectedEdges = Seq(Row(0L, 1L, "friend"), Row(1L, 2L, "friend"))
    val expectedVertices = Seq(Row(0L, "a", "f"), Row(1L, "b", "m"), Row(2L, "c", "m"))
    Seq(
      graph.filterEdges("relationship = 'friend'"),
      graph.filterEdges(col("relationship") === "friend")).foreach { filtered =>
      checkSchemaInvariants(graph, filtered)
      checkAnswer(filtered.vertices, vertexData)
      checkAnswer(filtered.edges, expectedEdges)

      val dropped = filtered.dropIsolatedVertices()
      checkSchemaInvariants(graph, dropped)
      checkAnswer(dropped.vertices, expectedVertices)
      checkAnswer(dropped.edges, expectedEdges)
    }
  }

  test("filterVertices") {
    val expectedVertices = Seq(Row(1L, "b", "m"), Row(2L, "c", "m"), Row(3L, "d", "f"))
    val expectedEdges = Seq(Row(1L, 2L, "friend"), Row(2L, 3L, "follow"))
    Seq(graph.filterVertices("id > 0"), graph.filterVertices(col(ID) > 0)).foreach { filtered =>
      checkSchemaInvariants(graph, filtered)
      checkAnswer(filtered.vertices, expectedVertices)
      checkAnswer(filtered.edges, expectedEdges)
    }
  }

  test("dropIsolatedVertices keeps vertices referenced only as destination") {
    val g = GraphFrame(vertexData, edgeData.where(col(SRC) === 2L && col(DST) === 3L))
    checkAnswer(
      g.dropIsolatedVertices().vertices,
      Seq(Row(2L, "c", "m"), Row(3L, "d", "f")))
  }

  test("asUndirected") {
    val g = graph.asUndirected()
    checkSchemaInvariants(graph, g)
    assert(g.edgeColumns.toSeq == Seq(SRC, DST, "relationship"))
    checkAnswer(g.vertices, vertexData)
    checkAnswer(
      g.edges,
      Seq(
        Row(0L, 1L, "friend"),
        Row(1L, 0L, "friend"),
        Row(1L, 0L, "follow"),
        Row(0L, 1L, "follow"),
        Row(1L, 2L, "friend"),
        Row(2L, 1L, "friend"),
        Row(2L, 3L, "follow"),
        Row(3L, 2L, "follow"),
        Row(2L, 0L, "unknown"),
        Row(0L, 2L, "unknown")))
  }

  test("asUndirected moves src and dst first and keeps attributes with dots in names") {
    val g = GraphFrame(
      vertexData,
      edgeData.select(col("relationship").as("the.relationship"), col(DST), col(SRC)))
    val undirected = g.asUndirected()
    assert(undirected.edgeColumns.toSeq == Seq(SRC, DST, "the.relationship"))
    checkAnswer(
      undirected.edges.where(col(SRC) === 3L),
      Seq(Row(3L, 2L, "follow")))
  }

  test("joinVertices") {
    val properties = spark
      .createDataFrame(Seq((0L, 10), (2L, 30), (5L, 50)))
      .toDF(ID, "age")
    val g = graph.joinVertices(properties)
    checkSchemaInvariants(graph, g)
    assert(g.vertexColumns.toSeq == Seq(ID, "name", "gender", "age"))
    checkAnswer(
      g.vertices,
      Seq(
        Row(0L, "a", "f", 10),
        Row(1L, "b", "m", null),
        Row(2L, "c", "m", 30),
        Row(3L, "d", "f", null)))
    checkAnswer(g.edges, edgeData)
  }

  test("joinEdges") {
    val properties = spark
      .createDataFrame(Seq((0L, 1L, 1.5), (2L, 3L, 2.5), (3L, 0L, 9.0)))
      .toDF(SRC, DST, "weight")
    val g = graph.joinEdges(properties)
    checkSchemaInvariants(graph, g)
    assert(g.edgeColumns.toSeq == Seq(SRC, DST, "relationship", "weight"))
    checkAnswer(
      g.edges,
      Seq(
        Row(0L, 1L, "friend", 1.5),
        Row(1L, 0L, "follow", null),
        Row(1L, 2L, "friend", null),
        Row(2L, 3L, "follow", 2.5),
        Row(2L, 0L, "unknown", null)))
    checkAnswer(g.vertices, vertexData)
  }

  test("joinVertices and joinEdges fail on invalid properties") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](graph.joinVertices(null)),
      condition = "GRAPH_NULL_DATAFRAME",
      sqlState = "22004",
      parameters = Map("dataFrameKind" -> "vertex properties"))
    checkError(
      exception = intercept[SparkIllegalArgumentException](graph.joinEdges(null)),
      condition = "GRAPH_NULL_DATAFRAME",
      sqlState = "22004",
      parameters = Map("dataFrameKind" -> "edge properties"))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinVertices(vertexData.select(col(ID).as("vid"), col("name").as("label")))
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`id`", "proposal" -> "`vid`, `label`"))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinEdges(edgeData.select(col(SRC), col("relationship").as("label")))
      },
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = "42703",
      parameters = Map("objectName" -> "`dst`", "proposal" -> "`src`, `label`"))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinVertices(vertexData.select(col(ID), col(ID), col("name").as("label")))
      },
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      sqlState = "42702",
      parameters = Map("name" -> "`id`", "n" -> "2"))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinVertices(
          vertexData.select(col(ID).cast(StringType).as(ID), col("name").as("label")))
      },
      condition = "GRAPH_PROPERTIES_KEY_TYPE_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "dataFrameKind" -> "vertex properties",
        "columnName" -> "`id`",
        "actualType" -> "\"STRING\"",
        "expectedType" -> "\"BIGINT\""))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinEdges(
          edgeData.select(col(SRC), col(DST).cast(IntegerType).as(DST), col("relationship")))
      },
      condition = "GRAPH_PROPERTIES_KEY_TYPE_MISMATCH",
      sqlState = "42K09",
      parameters = Map(
        "dataFrameKind" -> "edge properties",
        "columnName" -> "`dst`",
        "actualType" -> "\"INT\"",
        "expectedType" -> "\"BIGINT\""))
    checkError(
      exception = intercept[AnalysisException] {
        graph.joinVertices(vertexData.select(col(ID), col("name"), col("gender").as("label")))
      },
      condition = "COLUMN_ALREADY_EXISTS",
      sqlState = "42711",
      parameters = Map("columnName" -> "`name`"))
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          graph.joinEdges(
            edgeData.select(col(SRC), col(DST), col("relationship").as("RELATIONSHIP")))
        },
        condition = "COLUMN_ALREADY_EXISTS",
        sqlState = "42711",
        parameters = Map("columnName" -> "`RELATIONSHIP`"))
    }
  }

  test("operations on an empty graph") {
    val g = emptyGraph
    checkAnswer(g.filterVertices(col(ID) > 0).vertices, Seq.empty)
    checkAnswer(g.filterEdges(col(SRC) > 0).edges, Seq.empty)
    checkAnswer(g.dropIsolatedVertices().vertices, Seq.empty)
    checkAnswer(g.outDegrees, Seq.empty)
    checkAnswer(g.inDegrees, Seq.empty)
    checkAnswer(g.degrees, Seq.empty)
    checkAnswer(g.typeOutDegree("relationship", Seq("friend")), Seq.empty)
    checkAnswer(g.typeInDegree("relationship", Seq("friend")), Seq.empty)
    checkAnswer(g.typeDegree("relationship", Seq("friend")), Seq.empty)
  }

  test("toString puts the id, src and dst columns first") {
    val g = GraphFrame(
      vertexData.select("name", ID),
      edgeData.select("relationship", DST, SRC))
    assert(g.toString ==
      s"GraphFrame(v:${g.vertices.select(ID, "name")}, " +
        s"e:${g.edges.select(SRC, DST, "relationship")})")
  }

  test("vertices and edges cannot be used after deserialization") {
    val g = Utils.deserialize[GraphFrame](Utils.serialize(graph))
    checkError(
      exception = intercept[SparkException](g.vertices),
      condition = "CANNOT_INVOKE_IN_TRANSFORMATIONS",
      sqlState = "0A000",
      parameters = Map.empty)
    checkError(
      exception = intercept[SparkException](g.edges),
      condition = "CANNOT_INVOKE_IN_TRANSFORMATIONS",
      sqlState = "0A000",
      parameters = Map.empty)
  }

  test("string ids") {
    val g = GraphFrame(
      vertexData.withColumn(ID, col(ID).cast(StringType)),
      edgeData
        .withColumn(SRC, col(SRC).cast(StringType))
        .withColumn(DST, col(DST).cast(StringType)))
    g.validate()
    checkColumnType(g.vertices.schema, ID, StringType)
    checkAnswer(
      g.filterVertices(col(ID) =!= "0").edges.select(SRC, DST),
      Seq(Row("1", "2"), Row("2", "3")))
    checkAnswer(g.degrees, Seq(Row("0", 3), Row("1", 3), Row("2", 3), Row("3", 1)))
    checkAnswer(
      g.typeInDegree("relationship", Seq("follow")),
      Seq(Row("0", Row(1)), Row("1", Row(0)), Row("2", Row(0)), Row("3", Row(1))))
  }
}
