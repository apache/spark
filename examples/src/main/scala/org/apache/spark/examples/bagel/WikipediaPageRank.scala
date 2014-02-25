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

package org.apache.spark.examples.bagel

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

import scala.xml.{XML,NodeSeq}

/**
 * Run PageRank on XML Wikipedia dumps from http://wiki.freebase.com/wiki/WEX. Uses the "articles"
 * files from there, which contains one line per wiki article in a tab-separated format
 * (http://wiki.freebase.com/wiki/WEX/Documentation#articles).
 */
object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(
        "Usage: WikipediaPageRank <inputFile> <threshold> <numPartitions> <host> <usePartitioner>")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator",  classOf[PRKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numPartitions = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean

    sparkConf.setMaster(host).setAppName("WikipediaPageRank")
    val sc = new SparkContext(sparkConf)

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
            NodeSeq.Empty
          }
        }
      val outEdges = links.map(link => new String(link.text)).toArray
      val id = new String(title)
      (id, new PRVertex(1.0 / numVertices, outEdges))
    })
    if (usePartitioner) {
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    } else {
      vertices = vertices.cache
    }
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(String, PRMessage)]())
    val utils = new PageRankUtils
    val result =
        Bagel.run(
          sc, vertices, messages, combiner = new PRCombiner(),
          numPartitions = numPartitions)(
          utils.computeWithCombiner(numVertices, epsilon))

    // Print the result
    System.err.println("Articles with PageRank >= " + threshold + ":")
    val top =
      (result
       .filter { case (id, vertex) => vertex.value >= threshold }
       .map { case (id, vertex) => "%s\t%s\n".format(id, vertex.value) }
       .collect.mkString)
    println(top)
  }
}
