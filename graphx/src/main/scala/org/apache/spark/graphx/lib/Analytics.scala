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

package org.apache.spark.graphx.lib

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy._

/**
 * Driver program for running graph algorithms.
 */
object Analytics extends Logging {

  def main(args: Array[String]) = {
    val host = args(0)
    val taskType = args(1)
    val fname = args(2)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    def pickPartitioner(v: String): PartitionStrategy = {
      // TODO: Use reflection rather than listing all the partitioning strategies here.
      v match {
        case "RandomVertexCut" => RandomVertexCut
        case "EdgePartition1D" => EdgePartition1D
        case "EdgePartition2D" => EdgePartition2D
        case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
        case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + v)
      }
    }

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    taskType match {
      case "pagerank" =>
        var tol: Float = 0.001F
        var outFname = ""
        var numEPart = 4
        var partitionStrategy: Option[PartitionStrategy] = None

        options.foreach{
          case ("tol", v) => tol = v.toFloat
          case ("output", v) => outFname = v
          case ("numEPart", v) => numEPart = v.toInt
          case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(host, "PageRank(" + fname + ")", conf)

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          minEdgePartitions = numEPart).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = graph.pageRank(tol).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))

        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
          pr.map{case (id, r) => id + "\t" + r}.saveAsTextFile(outFname)
        }

        sc.stop()

      case "cc" =>
        var numIter = Int.MaxValue
        var numVPart = 4
        var numEPart = 4
        var isDynamic = false
        var partitionStrategy: Option[PartitionStrategy] = None

        options.foreach{
          case ("numIter", v) => numIter = v.toInt
          case ("dynamic", v) => isDynamic = v.toBoolean
          case ("numEPart", v) => numEPart = v.toInt
          case ("numVPart", v) => numVPart = v.toInt
          case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        if (!isDynamic && numIter == Int.MaxValue) {
          println("Set number of iterations!")
          sys.exit(1)
        }
        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(host, "ConnectedComponents(" + fname + ")", conf)
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          minEdgePartitions = numEPart).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        val cc = ConnectedComponents.run(graph)
        println("Components: " + cc.vertices.map{ case (vid,data) => data}.distinct())
        sc.stop()

      case "triangles" =>
        var numEPart = 4
        // TriangleCount requires the graph to be partitioned
        var partitionStrategy: PartitionStrategy = RandomVertexCut

        options.foreach{
          case ("numEPart", v) => numEPart = v.toInt
          case ("partStrategy", v) => partitionStrategy = pickPartitioner(v)
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }
        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")
        val sc = new SparkContext(host, "TriangleCount(" + fname + ")", conf)
        val graph = GraphLoader.edgeListFile(sc, fname, canonicalOrientation = true,
          minEdgePartitions = numEPart).partitionBy(partitionStrategy).cache()
        val triangles = TriangleCount.run(graph)
        println("Triangles: " + triangles.vertices.map {
          case (vid,data) => data.toLong
        }.reduce(_ + _) / 3)
        sc.stop()

      case _ =>
        println("Invalid task type.")
    }
  }
}
