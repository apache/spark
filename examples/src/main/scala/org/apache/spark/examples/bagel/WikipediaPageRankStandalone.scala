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

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML, NodeSeq}

import org.apache.spark._
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object WikipediaPageRankStandalone {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WikipediaPageRankStandalone <inputFile> <threshold> <numIterations> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.WPRSerializer")

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WikipediaPageRankStandalone")

    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    val links =
      if (usePartitioner)
        input.map(parseArticle _).partitionBy(partitioner).cache()
      else
        input.map(parseArticle _).cache()
    val n = links.count()
    val defaultRank = 1.0 / n
    val a = 0.15

    // Do the computation
    val startTime = System.currentTimeMillis
    val ranks =
        pageRank(links, numIterations, defaultRank, a, n, partitioner, usePartitioner, sc.defaultParallelism)

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) => "%s\t%s\n".format(id, rank) }
       .collect().mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }

  def parseArticle(line: String): (String, Array[String]) = {
    val fields = line.split("\t")
    val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
    val id = new String(title)
    val links =
      if (body == "\\N")
        NodeSeq.Empty
      else
        try {
          XML.loadString(body) \\ "link" \ "target"
        } catch {
          case e: org.xml.sax.SAXParseException =>
            System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
          NodeSeq.Empty
        }
    val outEdges = links.map(link => new String(link.text)).toArray
    (id, outEdges)
  }

  def pageRank(
    links: RDD[(String, Array[String])],
    numIterations: Int,
    defaultRank: Double,
    a: Double,
    n: Long,
    partitioner: Partitioner,
    usePartitioner: Boolean,
    numPartitions: Int
  ): RDD[(String, Double)] = {
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (linksWrapper, rankWrapper)) =>
          if (linksWrapper.length > 0) {
            if (rankWrapper.length > 0) {
              linksWrapper(0).map(dest => (dest, rankWrapper(0) / linksWrapper(0).size))
            } else {
              linksWrapper(0).map(dest => (dest, defaultRank / linksWrapper(0).size))
            }
          } else {
            Array[(String, Double)]()
          }
      }
      ranks = (contribs.combineByKey((x: Double) => x,
                                     (x: Double, y: Double) => x + y,
                                     (x: Double, y: Double) => x + y,
                                     partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }
    ranks
  }
}

class WPRSerializer extends org.apache.spark.serializer.Serializer {
  def newInstance(): SerializerInstance = new WPRSerializerInstance()
}

class WPRSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): ByteBuffer = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    throw new UnsupportedOperationException()
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    throw new UnsupportedOperationException()
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new WPRSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new WPRDeserializationStream(s)
  }
}

class WPRSerializationStream(os: OutputStream) extends SerializationStream {
  val dos = new DataOutputStream(os)

  def writeObject[T](t: T): SerializationStream = t match {
    case (id: String, wrapper: ArrayBuffer[_]) => wrapper(0) match {
      case links: Array[String] => {
        dos.writeInt(0) // links
        dos.writeUTF(id)
        dos.writeInt(links.length)
        for (link <- links) {
          dos.writeUTF(link)
        }
        this
      }
      case rank: Double => {
        dos.writeInt(1) // rank
        dos.writeUTF(id)
        dos.writeDouble(rank)
        this
      }
    }
    case (id: String, rank: Double) => {
      dos.writeInt(2) // rank without wrapper
      dos.writeUTF(id)
      dos.writeDouble(rank)
      this
    }
  }

  def flush() { dos.flush() }
  def close() { dos.close() }
}

class WPRDeserializationStream(is: InputStream) extends DeserializationStream {
  val dis = new DataInputStream(is)

  def readObject[T](): T = {
    val typeId = dis.readInt()
    typeId match {
      case 0 => {
        val id = dis.readUTF()
        val numLinks = dis.readInt()
        val links = new Array[String](numLinks)
        for (i <- 0 until numLinks) {
          val link = dis.readUTF()
          links(i) = link
        }
        (id, ArrayBuffer(links)).asInstanceOf[T]
      }
      case 1 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, ArrayBuffer(rank)).asInstanceOf[T]
      }
      case 2 => {
        val id = dis.readUTF()
        val rank = dis.readDouble()
        (id, rank).asInstanceOf[T]
     }
    }
  }

  def close() { dis.close() }
}
