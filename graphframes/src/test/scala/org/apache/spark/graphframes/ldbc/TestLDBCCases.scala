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

package org.apache.spark.graphframes.ldbc

import java.io.File
import java.nio.file._
import java.util.Properties

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.examples.LDBCUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.abs
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class TestLDBCCases extends SparkFunSuite with GraphFrameTestSparkContext {
  private val resourcesPath = Paths.get(new File("target").toURI)
  private val unreachableID = 9223372036854775807L

  // These upstream integration tests download Graphalytics fixtures and invoke curl, zstd, and tar.
  private def ldbcTest(name: String)(body: => Any): Unit = {
    if (sys.env.get("SPARK_RUN_LDBC_GRAPH_TESTS").contains("1")) {
      test(name)(body)
    } else {
      ignore(name)(body)
    }
  }

  private def readUndirectedUnweighted(pathPrefix: String): GraphFrame = {
    var edges = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .csv(s"${pathPrefix}.e")
      .toDF("src", "dst")

    // TODO: replace by symmetrize when graphframes/graphframes#548 is done!
    edges = edges
      .select("src", "dst")
      .union(edges.select(col("dst").alias("src"), col("src").alias("dst")))

    val nodes = spark.read
      .text(s"${pathPrefix}.v")
      .toDF("id")
      .select(col("id").cast(LongType))

    GraphFrame(nodes, edges)
  }

  private def readDirectedUnweighted(pathPrefix: String): GraphFrame = {
    val edges = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .csv(s"${pathPrefix}.e")
      .toDF("src", "dst")

    val nodes = spark.read
      .text(s"${pathPrefix}.v")
      .toDF("id")
      .select(col("id").cast(LongType))

    GraphFrame(nodes, edges)
  }

  private def readProperties(path: Path): Properties = {
    val props = new Properties()
    val stream = Files.newInputStream(path)
    props.load(stream)
    stream.close()
    props
  }

  private lazy val ldbcTestBFSDirected: (GraphFrame, DataFrame, Long) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_BFS_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_BFS_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_BFS_UNDIRECTED}-BFS")

    val expectedDistances = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("distance", IntegerType))))
      .csv(expectedPath.toString)
      .toDF("id", "distance")
    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_BFS_UNDIRECTED}.properties"))
    (
      readDirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_BFS_UNDIRECTED}"),
      expectedDistances,
      props.getProperty(s"graph.${LDBCUtils.TEST_BFS_UNDIRECTED}.bfs.source-vertex").toLong)
  }

  Seq("graphframes", "graphx").foreach { algo =>
    ldbcTest(s"test undirected BFS with LDBC for impl ${algo}") {
      val testCase = ldbcTestBFSDirected
      val srcVertex = testCase._3

      // this graph is undirected, but in GF direction exists
      // only on the level of algorithms!
      val spResult = testCase._1.shortestPaths
        .landmarks(Seq(srcVertex))
        .setAlgorithm(algo)
        .setIsDirected(false)
        .run()
        .select(
          col(GraphFrame.ID),
          col("distances").getItem(srcVertex).cast(LongType).alias("got_distance"))
        .na
        .fill(Map("got_distance" -> unreachableID))

      assert(spResult.count() == testCase._1.vertices.count())
      assert(
        spResult
          .join(testCase._2, Seq("id"), "left")
          .filter(col("got_distance") =!= col("distance"))
          .collect()
          .isEmpty)

    }
  }

  private lazy val ldbcTestCDLPUndirected: (GraphFrame, DataFrame, Int) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_CDLP_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_CDLP_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_CDLP_UNDIRECTED}-CDLP")

    val expectedCommunities = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("community", LongType))))
      .csv(expectedPath.toString)
      .toDF("id", "community")
    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_CDLP_UNDIRECTED}.properties"))
    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_CDLP_UNDIRECTED}"),
      expectedCommunities,
      props.getProperty(s"graph.${LDBCUtils.TEST_CDLP_UNDIRECTED}.cdlp.max-iterations").toInt)
  }

  Seq("graphx", "graphframes").foreach { algo =>
    ldbcTest(s"test undirected CDLP with LDBC for algo ${algo}") {
      val testCase = ldbcTestCDLPUndirected
      val cdlpResults = testCase._1.labelPropagation.setAlgorithm(algo).maxIter(testCase._3).run()
      assert(cdlpResults.count() == testCase._1.vertices.count())
      assert(
        cdlpResults
          .join(testCase._2, Seq("id"), "left")
          .filter(col("label") =!= col("community"))
          .collect()
          .isEmpty)
    }
  }

  private lazy val ldbcTestPageRankUndirected: (GraphFrame, DataFrame, Double, Int) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_PR_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_PR_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_PR_UNDIRECTED}-PR")

    val expectedRanks = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("pr", DoubleType))))
      .csv(expectedPath.toString)
      .toDF("id", "pr")

    val props = readProperties(caseRoot.resolve(s"${LDBCUtils.TEST_PR_UNDIRECTED}.properties"))
    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_PR_UNDIRECTED}"),
      expectedRanks,
      props.getProperty(s"graph.${LDBCUtils.TEST_PR_UNDIRECTED}.pr.damping-factor").toDouble,
      props.getProperty(s"graph.${LDBCUtils.TEST_PR_UNDIRECTED}.pr.num-iterations").toInt)
  }

  // TODO: add graphframes after finishing graphframes/graphframes#569
  Seq("graphx").foreach { algo =>
    ldbcTest(s"test undirected PR with LDBC for algo ${algo}") {
      val testCase = ldbcTestPageRankUndirected
      val prResults = testCase._1.pageRank
        .resetProbability(1.0 - testCase._3)
        .maxIter(testCase._4)
        .run()
        .vertices

      // Normalize??
      val sumPR = prResults.agg(sum(col("pagerank"))).collect().head.getAs[Double](0)
      val prResultsNormalized = prResults.withColumn("pagerank", col("pagerank") / lit(sumPR))
      assert(prResults.count() == testCase._1.vertices.count())
      assert(
        prResultsNormalized
          .join(testCase._2, Seq("id"), "left")
          .filter(abs(col("pagerank") - col("pr")) >= lit(1e-4))
          .collect()
          .isEmpty)
    }
  }

  private lazy val ldbcTestWCCUndirected: (GraphFrame, DataFrame) = {
    LDBCUtils.downloadLDBCIfNotExists(resourcesPath, LDBCUtils.TEST_WCC_UNDIRECTED)
    val caseRoot = resourcesPath.resolve(LDBCUtils.TEST_WCC_UNDIRECTED)

    val expectedPath = caseRoot.resolve(s"${LDBCUtils.TEST_WCC_UNDIRECTED}-WCC")

    val expectedComponents = spark.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(StructType(Seq(StructField("id", LongType), StructField("wcomp", LongType))))
      .csv(expectedPath.toString)
      .toDF("id", "wcomp")

    (
      readUndirectedUnweighted(s"${caseRoot.toString}/${LDBCUtils.TEST_WCC_UNDIRECTED}"),
      expectedComponents)
  }

  Seq("two_phase", "graphx", "randomized_contraction").foreach { algo =>
    ldbcTest(s"test undirected WCC with LDBC for impl ${algo}") {
      val testCase = ldbcTestWCCUndirected
      var cc = testCase._1.connectedComponents.setAlgorithm(algo)
      if (algo == "randomized_contraction") {
        // RC is randomized by it's nature;
        cc = cc.setUseLabelsAsComponents(true)
      }
      val ccResults = cc.run()
      assert(ccResults.count() == testCase._1.vertices.count())
      assert(
        ccResults
          .join(testCase._2, Seq("id"), "left")
          .filter(col("wcomp") =!= col("component"))
          .collect()
          .isEmpty)
    }
  }
}
