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

// scalastyle:off println
package org.apache.spark.examples.graphx

import scala.collection.{immutable, mutable}

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx.lib._
import org.apache.spark.storage.StorageLevel


/**
 * Driver program for running graph algorithms.
 */
object Analytics {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      val usage = """Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions>
      |[other options] Supported 'taskType' as follows:
      |pagerank    Compute PageRank
      |cc          Compute the connected components of vertices
      |triangles   Count the number of triangles""".stripMargin
      System.err.println(usage)
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException(s"Invalid argument: $arg")
      }
    }
    val options = mutable.Map(immutable.ArraySeq.unsafeWrapArray(optionsList): _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName(s"PageRank($fname)"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println(s"GRAPHX: Number of vertices ${graph.vertices.count()}")
        println(s"GRAPHX: Number of edges ${graph.edges.count()}")

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println(s"GRAPHX: Total rank: ${pr.map(_._2).reduce(_ + _)}")

        if (!outFname.isEmpty) {
          println(s"Saving pageranks of pages to $outFname")
          pr.map { case (id, r) => s"$id\t$r" }.saveAsTextFile(outFname)
        }

        sc.stop()

      case "cc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
        }

        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName(s"ConnectedComponents($fname)"))
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        val cc = ConnectedComponents.run(graph)
        println(s"Components: ${cc.vertices.map { case (vid, data) => data }.distinct()}")
        sc.stop()

      case "triangles" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
        }

        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName(s"TriangleCount($fname)"))
        val graph = GraphLoader.edgeListFile(sc, fname,
          canonicalOrientation = true,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel)
          // TriangleCount requires the graph to be partitioned
          .partitionBy(partitionStrategy.getOrElse(RandomVertexCut)).cache()
        val triangles = TriangleCount.run(graph)
        val triangleTypes = triangles.vertices.map {
          case (vid, data) => data.toLong
        }.reduce(_ + _) / 3

        println(s"Triangles: ${triangleTypes}")
        sc.stop()

      case _ =>
        println("Invalid task type.")
    }
  }
}
// scalastyle:on println
