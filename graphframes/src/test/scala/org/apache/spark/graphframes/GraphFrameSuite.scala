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

package org.apache.spark.graphframes

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.examples.Graphs

import java.io.File
import java.nio.file.Files

class GraphFrameSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import GraphFrame._

  var vertices: DataFrame = _
  val localVertices: Map[Long, String] = Map(1L -> "A", 2L -> "B", 3L -> "C")
  val localEdges: Map[(Long, Long), String] =
    Map((1L, 2L) -> "love", (2L, 1L) -> "hate", (2L, 3L) -> "follow")
  var edges: DataFrame = _
  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory(null).toFile()
    vertices = spark.createDataFrame(localVertices.toSeq).toDF("id", "name")
    edges = spark
      .createDataFrame(localEdges.toSeq.map { case ((src, dst), action) =>
        (src, dst, action)
      })
      .toDF("src", "dst", "action")
  }

  override def afterAll(): Unit = {
    FileUtils.deleteQuietly(tempDir)
    super.afterAll()
  }

  test("test validate") {
    val goodG = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L))).toDF("src", "dst"))
    goodG.validate() // no exception should be thrown

    val notDistinctVertices = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (1L, "d"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L))).toDF("src", "dst"))
    assertThrows[InvalidGraphException](notDistinctVertices.validate())

    val missingVertices = GraphFrame(
      spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "attr"),
      spark.createDataFrame(Seq((1L, 2L), (2L, 1L), (2L, 3L), (1L, 4L))).toDF("src", "dst"))
    assertThrows[InvalidGraphException](missingVertices.validate())
  }

  test("construction from DataFrames") {
    val g = GraphFrame(vertices, edges)
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    intercept[IllegalArgumentException] {
      val badVertices = vertices.select(col("id").as("uid"), col("name"))
      GraphFrame(badVertices, edges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src").as("srcId"), col("dst"), col("action"))
      GraphFrame(vertices, badEdges)
    }
    intercept[IllegalArgumentException] {
      val badEdges = edges.select(col("src"), col("dst").as("dstId"), col("action"))
      GraphFrame(vertices, badEdges)
    }
  }

  test("construction from DataFrames with dots in column names") {
    val g = GraphFrame(
      vertices.withColumnRenamed("name", "a.name"),
      edges.withColumnRenamed("action", "the.action"))
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.pageRank.maxIter(10).run()
  }

  test("construction from DataFrames with backquote in column names") {
    val g = GraphFrame(
      vertices.withColumnRenamed("name", "a `name`"),
      edges.withColumnRenamed("action", "the `action`"))
    g.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    g.pageRank.maxIter(10).run()
  }

  test("construction from edge DataFrame") {
    val g = GraphFrame.fromEdges(edges)
    assert(g.vertices.columns === Array("id"))
    val idsFromVertices = g.vertices.select("id").rdd.map(_.getLong(0)).collect()
    val idsFromVerticesSet = idsFromVertices.toSet
    assert(idsFromVertices.length === idsFromVerticesSet.size)
    val idsFromEdgesSet = g.edges
      .select("src", "dst")
      .rdd
      .flatMap {
        case Row(src: Long, dst: Long) =>
          Seq(src, dst)
        case _: Row => throw new GraphFramesUnreachableException()
      }
      .collect()
      .toSet
    assert(idsFromVerticesSet === idsFromEdgesSet)
    g.vertices.unpersist()
  }

  test("construction from edges DataFrame, string IDs") {
    val edges = spark
      .createDataFrame(Seq(("a", "b", "love"), ("b", "a", "hate"), ("b", "c", "follow")))
      .toDF("src", "dst", "action")

    val g = GraphFrame.fromEdges(edges)
    assert(g.vertices.columns === Array("id"))
    val idsFromVertices = g.vertices.select("id").rdd.map(_.getString(0)).collect()
    val idsFromVerticesSet = idsFromVertices.toSet
    assert(idsFromVertices.length === idsFromVerticesSet.size)
    val idsFromEdgesSet = g.edges
      .select("src", "dst")
      .rdd
      .flatMap {
        case Row(src: String, dst: String) =>
          Seq(src, dst)
        case _: Row => throw new GraphFramesUnreachableException()
      }
      .collect()
      .toSet
    assert(idsFromVerticesSet === idsFromEdgesSet)
    g.vertices.unpersist()
  }

  test("construction from edges DataFrame, different storage level") {
    val edges = spark
      .createDataFrame(Seq((1L, 2L, "love"), (2L, 1L, "hate"), (2L, 3L, "follow")))
      .toDF("src", "dst", "action")

    var g = GraphFrame.fromEdges(edges)
    assert(g.vertices.storageLevel === StorageLevel.MEMORY_AND_DISK)
    g.vertices.unpersist()

    g = GraphFrame.fromEdges(edges, StorageLevel.MEMORY_AND_DISK_SER)
    assert(g.vertices.storageLevel === StorageLevel.MEMORY_AND_DISK_SER)
    g.vertices.unpersist()
  }

  test("construction from GraphX") {
    val vv: RDD[(Long, String)] = vertices.rdd.map {
      case Row(id: Long, name: String) =>
        (id, name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    val ee: RDD[Edge[String]] = edges.rdd.map {
      case Row(src: Long, dst: Long, action: String) =>
        Edge(src, dst, action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    val g = Graph[String, String](vv, ee)
    val gf = GraphFrame.fromGraphX(g)
    gf.vertices.select("id", "attr").collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _: Row => throw new GraphFramesUnreachableException()
    }
    gf.edges.select("src", "dst", "attr").collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _: Row => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: Long IDs") {
    val gf = GraphFrame(vertices, edges)
    val g = gf.toGraphX
    g.vertices.collect().foreach {
      case (id0, Row(id1: Long, name: String)) =>
        assert(id0 === id1)
        assert(localVertices(id0) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Edge(src0, dst0, Row(src1: Long, dst1: Long, action: String)) =>
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: Int IDs") {
    val vv = vertices.select(col("id").cast(IntegerType).as("id"), col("name"))
    val ee = edges.select(
      col("src").cast(IntegerType).as("src"),
      col("dst").cast(IntegerType).as("dst"),
      col("action"))
    val gf = GraphFrame(vv, ee)
    val g = gf.toGraphX
    // Int IDs should be directly cast to Long, so ID values should match.
    val vCols = gf.vertexColumnMap
    val eCols = gf.edgeColumnMap
    g.vertices.collect().foreach {
      case (id0: Long, attr: Row) =>
        val id1 = attr.getInt(vCols("id"))
        val name = attr.getString(vCols("name"))
        assert(id0 === id1)
        assert(localVertices(id0) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g.edges.collect().foreach {
      case Edge(src0: Long, dst0: Long, attr: Row) =>
        val src1 = attr.getInt(eCols("src"))
        val dst1 = attr.getInt(eCols("dst"))
        val action = attr.getString(eCols("action"))
        assert(src0 === src1)
        assert(dst0 === dst1)
        assert(localEdges((src0, dst0)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("convert to GraphX: String IDs") {
    try {
      val vv = vertices.select(col("id").cast(StringType).as("id"), col("name"))
      val ee = edges.select(
        col("src").cast(StringType).as("src"),
        col("dst").cast(StringType).as("dst"),
        col("action"))
      val gf = GraphFrame(vv, ee)
      val g = gf.toGraphX
      // String IDs will be re-indexed, so ID values may not match.
      val vCols = gf.vertexColumnMap
      val eCols = gf.edgeColumnMap
      // First, get index.
      val new2oldID: Map[Long, String] = g.vertices
        .map { case (id: Long, attr: Row) =>
          (id, attr.getString(vCols("id")))
        }
        .collect()
        .toMap
      // Same as in test with Int IDs, but with re-indexing
      g.vertices.collect().foreach { case (id0: Long, attr: Row) =>
        val id1 = attr.getString(vCols("id"))
        val name = attr.getString(vCols("name"))
        assert(new2oldID(id0) === id1)
        assert(localVertices(new2oldID(id0).toLong) === name)
      }
      g.edges.collect().foreach { case Edge(src0: Long, dst0: Long, attr: Row) =>
        val src1 = attr.getString(eCols("src"))
        val dst1 = attr.getString(eCols("dst"))
        val action = attr.getString(eCols("action"))
        assert(new2oldID(src0) === src1)
        assert(new2oldID(dst0) === dst1)
        assert(localEdges((new2oldID(src0).toLong, new2oldID(dst0).toLong)) === action)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  test("save/load") {
    val g0 = GraphFrame(vertices, edges)
    val vPath = new Path(tempDir.getPath, "vertices").toString
    val ePath = new Path(tempDir.getPath, "edges").toString
    g0.vertices.write.parquet(vPath)
    g0.edges.write.parquet(ePath)

    val v1 = spark.read.parquet(vPath)
    val e1 = spark.read.parquet(ePath)
    val g1 = GraphFrame(v1, e1)

    g1.vertices.collect().foreach {
      case Row(id: Long, name: String) =>
        assert(localVertices(id) === name)
      case _ => throw new GraphFramesUnreachableException()
    }
    g1.edges.collect().foreach {
      case Row(src: Long, dst: Long, action: String) =>
        assert(localEdges((src, dst)) === action)
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("degree metrics") {
    val g = GraphFrame(vertices, edges)

    assert(g.outDegrees.columns === Seq("id", "outDegree"))
    val outDegrees = g.outDegrees
      .collect()
      .map {
        case Row(id: Long, outDeg: Int) =>
          (id, outDeg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(outDegrees === Map(1L -> 1, 2L -> 2))

    assert(g.inDegrees.columns === Seq("id", "inDegree"))
    val inDegrees = g.inDegrees
      .collect()
      .map {
        case Row(id: Long, inDeg: Int) =>
          (id, inDeg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(inDegrees === Map(1L -> 1, 2L -> 1, 3L -> 1))

    assert(g.degrees.columns === Seq("id", "degree"))
    val degrees = g.degrees
      .collect()
      .map {
        case Row(id: Long, deg: Int) =>
          (id, deg)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toMap
    assert(degrees === Map(1L -> 2, 2L -> 3, 3L -> 1))
  }

  test("type degree metrics") {
    val g = GraphFrame(vertices, edges)

    assert(g.typeOutDegree("action").columns === Seq("id", "outDegrees"))
    val typeOutDegrees = g.typeOutDegree("action").collect()

    val outDegreesSchema =
      g.typeOutDegree("action").schema("outDegrees").dataType.asInstanceOf[StructType]
    val outDegreesFieldNames = outDegreesSchema.fields.map(_.name).toSet
    assert(outDegreesFieldNames === Set("love", "hate", "follow"))

    val typeOutDegMap = typeOutDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeOutDegMap(1L).getAs[Int]("love") === 1)
    assert(typeOutDegMap(1L).getAs[Int]("hate") === 0)
    assert(typeOutDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeOutDegMap(2L).getAs[Int]("love") === 0)
    assert(typeOutDegMap(2L).getAs[Int]("hate") === 1)
    assert(typeOutDegMap(2L).getAs[Int]("follow") === 1)

    assert(g.typeInDegree("action").columns === Seq("id", "inDegrees"))
    val typeInDegrees = g.typeInDegree("action").collect()

    val inDegreesSchema =
      g.typeInDegree("action").schema("inDegrees").dataType.asInstanceOf[StructType]
    val inDegreesFieldNames = inDegreesSchema.fields.map(_.name).toSet
    assert(inDegreesFieldNames === Set("love", "hate", "follow"))

    val typeInDegMap = typeInDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeInDegMap(1L).getAs[Int]("love") === 0)
    assert(typeInDegMap(1L).getAs[Int]("hate") === 1)
    assert(typeInDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeInDegMap(2L).getAs[Int]("love") === 1)
    assert(typeInDegMap(2L).getAs[Int]("hate") === 0)
    assert(typeInDegMap(2L).getAs[Int]("follow") === 0)

    assert(typeInDegMap(3L).getAs[Int]("love") === 0)
    assert(typeInDegMap(3L).getAs[Int]("hate") === 0)
    assert(typeInDegMap(3L).getAs[Int]("follow") === 1)

    assert(g.typeDegree("action").columns === Seq("id", "degrees"))
    val typeDegrees = g.typeDegree("action").collect()

    val degreesSchema = g.typeDegree("action").schema("degrees").dataType.asInstanceOf[StructType]
    val degreesFieldNames = degreesSchema.fields.map(_.name).toSet
    assert(degreesFieldNames === Set("love", "hate", "follow"))

    val typeDegMap = typeDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeDegMap(1L).getAs[Int]("love") === 1)
    assert(typeDegMap(1L).getAs[Int]("hate") === 1)
    assert(typeDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeDegMap(2L).getAs[Int]("love") === 1)
    assert(typeDegMap(2L).getAs[Int]("hate") === 1)
    assert(typeDegMap(2L).getAs[Int]("follow") === 1)

    assert(typeDegMap(3L).getAs[Int]("love") === 0)
    assert(typeDegMap(3L).getAs[Int]("hate") === 0)
    assert(typeDegMap(3L).getAs[Int]("follow") === 1)
  }

  test("type degree metrics with explicit edge types") {
    val g = GraphFrame(vertices, edges)
    val edgeTypes = Seq("love", "hate", "follow")

    val typeOutDegrees = g.typeOutDegree("action", Some(edgeTypes)).collect()

    val typeOutDegMap = typeOutDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeOutDegMap(1L).getAs[Int]("love") === 1)
    assert(typeOutDegMap(1L).getAs[Int]("hate") === 0)
    assert(typeOutDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeOutDegMap(2L).getAs[Int]("love") === 0)
    assert(typeOutDegMap(2L).getAs[Int]("hate") === 1)
    assert(typeOutDegMap(2L).getAs[Int]("follow") === 1)

    val typeInDegrees = g.typeInDegree("action", Some(edgeTypes)).collect()
    val typeInDegMap = typeInDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeInDegMap(1L).getAs[Int]("love") === 0)
    assert(typeInDegMap(1L).getAs[Int]("hate") === 1)
    assert(typeInDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeInDegMap(2L).getAs[Int]("love") === 1)
    assert(typeInDegMap(2L).getAs[Int]("hate") === 0)
    assert(typeInDegMap(2L).getAs[Int]("follow") === 0)

    assert(typeInDegMap(3L).getAs[Int]("love") === 0)
    assert(typeInDegMap(3L).getAs[Int]("hate") === 0)
    assert(typeInDegMap(3L).getAs[Int]("follow") === 1)

    val typeDegrees = g.typeDegree("action", Some(edgeTypes)).collect()
    val typeDegMap = typeDegrees.map { row =>
      val id = row.getLong(0)
      val degrees = row.getStruct(1)
      (id, degrees)
    }.toMap

    assert(typeDegMap(1L).getAs[Int]("love") === 1)
    assert(typeDegMap(1L).getAs[Int]("hate") === 1)
    assert(typeDegMap(1L).getAs[Int]("follow") === 0)

    assert(typeDegMap(2L).getAs[Int]("love") === 1)
    assert(typeDegMap(2L).getAs[Int]("hate") === 1)
    assert(typeDegMap(2L).getAs[Int]("follow") === 1)

    assert(typeDegMap(3L).getAs[Int]("love") === 0)
    assert(typeDegMap(3L).getAs[Int]("hate") === 0)
    assert(typeDegMap(3L).getAs[Int]("follow") === 1)
  }

  test("cache") {
    val g = GraphFrame(vertices, edges)

    g.persist(StorageLevel.MEMORY_ONLY)

    g.unpersist()
    // org.apache.spark.sql.execution.columnar.InMemoryRelation is private and not accessible
    // This has prevented us from validating DataFrame's are cached.
  }

  test("basic operations on an empty graph") {
    for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
      assert(empty.inDegrees.count() === 0L)
      assert(empty.outDegrees.count() === 0L)
      assert(empty.degrees.count() === 0L)
      assert(empty.triplets.count() === 0L)
    }
  }

  test("test triplets") {
    // Basic triplets test
    val g = GraphFrame(vertices, edges)
    val triplets = g.triplets.collect()
    assert(triplets.length === localEdges.size)
    triplets.foreach {
      case Row(src: Row, edge: Row, dst: Row) =>
        assert(src.getLong(0) === edge.getLong(0)) // src.id === edge.src
        assert(dst.getLong(0) === edge.getLong(1)) // dst.id === edge.dst
        assert(localEdges((edge.getLong(0), edge.getLong(1))) === edge.getString(2))
      case _ => throw new GraphFramesUnreachableException()
    }

    // Test with attributes
    val v2 = vertices.withColumn("age", lit(10))
    val e2 = edges.withColumn("weight", lit(2.0))
    val g2 = GraphFrame(v2, e2)
    val triplets2 = g2.triplets.collect()
    triplets2.foreach {
      case Row(src: Row, edge: Row, _: Row) =>
        assert(src.getInt(2) === 10) // Check vertex attribute
        assert(edge.getDouble(3) === 2.0) // Check edge attribute
      case _ => throw new GraphFramesUnreachableException()
    }

    // Test with dots in column names
    val v3 = v2.withColumnRenamed("age", "person.age")
    val e3 = e2.withColumnRenamed("weight", "edge.weight")
    val g3 = GraphFrame(v3, e3)
    val triplets3 = g3.triplets.collect()
    triplets3.foreach {
      case Row(src: Row, edge: Row, _: Row) =>
        assert(src.getInt(2) === 10) // Check vertex attribute
        assert(edge.getDouble(3) === 2.0) // Check edge attribute
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  test("nestAsCol with dots in column names") {
    val df = vertices.withColumnRenamed("name", "a.name")
    val col = nestAsCol(df, "attr")
    assert(
      df.select(col).schema === StructType(
        Seq(
          StructField(
            "attr",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("a.name", StringType, nullable = true))),
            nullable = false))))
  }

  test("nestAsCol with backquote in column names") {
    val df = vertices.withColumnRenamed("name", "a `name`")
    val col = nestAsCol(df, "attr")
    assert(
      df.select(col).schema === StructType(
        Seq(
          StructField(
            "attr",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("a `name`", StringType, nullable = true))),
            nullable = false))))
  }

  test("power iteration clustering wrapper") {
    val spark = this.spark
    import spark.implicits._
    val edges = spark
      .createDataFrame(
        Seq(
          (1, 0, 0.5),
          (2, 0, 0.5),
          (2, 1, 0.7),
          (3, 0, 0.5),
          (3, 1, 0.7),
          (3, 2, 0.9),
          (4, 0, 0.5),
          (4, 1, 0.7),
          (4, 2, 0.9),
          (4, 3, 1.1),
          (5, 0, 0.5),
          (5, 1, 0.7),
          (5, 2, 0.9),
          (5, 3, 1.1),
          (5, 4, 1.3)))
      .toDF("src", "dst", "weight")
    val vertices = Seq(0, 1, 2, 3, 4, 5).toDF("id")
    val gf = GraphFrame(vertices, edges)
    val clusters = gf
      .powerIterationClustering(k = 2, maxIter = 40, weightCol = Some("weight"))
      .collect()
      .sortBy(_.getAs[Long]("id"))
      .map(_.getAs[Int]("cluster"))
      .toSeq
    assert(Seq(0, 0, 0, 0, 1, 0) == clusters)
  }

  test("power iteration clustering string ids") {
    val spark = this.spark
    import spark.implicits._
    val edges = spark
      .createDataFrame(
        Seq(
          ("1", "0", 0.5),
          ("2", "0", 0.5),
          ("2", "1", 0.7),
          ("3", "0", 0.5),
          ("3", "1", 0.7),
          ("3", "2", 0.9),
          ("4", "0", 0.5),
          ("4", "1", 0.7),
          ("4", "2", 0.9),
          ("4", "3", 1.1),
          ("5", "0", 0.5),
          ("5", "1", 0.7),
          ("5", "2", 0.9),
          ("5", "3", 1.1),
          ("5", "4", 1.3)))
      .toDF("src", "dst", "weight")
    val vertices = Seq("0", "1", "2", "3", "4", "5").toDF("id")
    val gf = GraphFrame(vertices, edges)
    val clusters = gf
      .powerIterationClustering(k = 2, maxIter = 40, weightCol = Some("weight"))
      .collect()
      .sortBy(_.getAs[String]("id"))
      .map(_.getAs[Int]("cluster"))
      .toSeq
    assert(Seq(1, 1, 1, 1, 1, 0) == clusters)
  }

  test("convert directed graph to undirected") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L), (2L, 3L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val undirected = g.asUndirected()

    // Check edge count doubled
    assert(undirected.edges.count() === 2 * g.edges.count())

    // Verify reverse edges exist
    val edges = undirected.edges.sort("src", "dst").collect()
    assert(edges.length === 4)
    assert(edges(0).getLong(0) === 1L)
    assert(edges(0).getLong(1) === 2L)
    assert(edges(1).getLong(0) === 2L)
    assert(edges(1).getLong(1) === 1L)
    assert(edges(2).getLong(0) === 2L)
    assert(edges(2).getLong(1) === 3L)
    assert(edges(3).getLong(0) === 3L)
    assert(edges(3).getLong(1) === 2L)
  }

  test("reverse directed graph edges") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L), (2L, 3L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val reversed = g.asReversed()

    // Check edge count is the same
    assert(reversed.edges.count() === g.edges.count())

    // Verify edges are reversed
    val edges = reversed.edges.sort("src", "dst").collect()
    assert(edges.length === 2)
    assert(edges(0).getLong(0) === 2L)
    assert(edges(0).getLong(1) === 1L)
    assert(edges(1).getLong(0) === 3L)
    assert(edges(1).getLong(1) === 2L)
  }

  test("reverse directed graph edges with attributes") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L, "edge1"))).toDF("src", "dst", "attr")
    val g = GraphFrame(v, e)
    val reversed = g.asReversed()

    val edges = reversed.edges.collect()
    assert(edges.length === 1)
    assert(edges(0).getLong(0) === 2L)
    assert(edges(0).getLong(1) === 1L)
    assert(edges(0).getString(2) === "edge1")
  }

  test("toGraphX should throw IllegalArgumentException for null IDs") {
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("attr", StringType, nullable = true)))

    val data = spark.sparkContext.parallelize(Seq(Row(1L, "a"), Row(null, "b")))

    val vertices = spark.createDataFrame(data, schema)
    val edges = spark.createDataFrame(Seq((1L, 1L, "friend"))).toDF("src", "dst", "relationship")

    val g = GraphFrame(vertices, edges)

    val e = intercept[org.apache.spark.SparkException] {
      // Trigger an action to hit the lazy exception in the .map
      g.toGraphX.vertices.collect()
    }

    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getMessage.contains("Vertex ID cannot be null"))
  }

  test("toGraphX should throw IllegalArgumentException for null Edge Src/Dst") {
    val vertices = spark.createDataFrame(Seq((1L, "a"))).toDF("id", "attr")

    val edgeSchema = StructType(
      Seq(
        StructField("src", LongType, nullable = true),
        StructField("dst", LongType, nullable = true),
        StructField("relationship", StringType, nullable = true)))

    val edgeData = spark.sparkContext.parallelize(Seq(Row(1L, null, "friend")))

    val edges = spark.createDataFrame(edgeData, edgeSchema)

    val g = GraphFrame(vertices, edges)

    val e = intercept[org.apache.spark.SparkException] {
      // Trigger action on edges
      g.toGraphX.edges.collect()
    }

    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getMessage.contains("Edge"))
    assert(e.getMessage.contains("cannot be null"))
  }

  test("convert directed graph with edge attributes to undirected") {
    val v = spark.createDataFrame(Seq((1L, "a"), (2L, "b"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((1L, 2L, "edge1"))).toDF("src", "dst", "attr")
    val g = GraphFrame(v, e)
    val undirected = g.asUndirected()

    val edges = undirected.edges.collect()
    assert(edges.length === 2)
    assert(
      edges.exists(r => r.getLong(0) == 1L && r.getLong(1) == 2L && r.getString(2) == "edge1"))
    assert(
      edges.exists(r => r.getLong(0) == 2L && r.getLong(1) == 1L && r.getString(2) == "edge1"))
  }
}
