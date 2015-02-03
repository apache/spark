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

package org.apache.spark.graphx.loaders

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge

/**
 * Provides utilities for loading RDF [[Graph]]s from .NT dumps.
 */
object RDFLoader extends Logging {
	
  val relregex = "<([^>]+)>\\s<([^>]+)>\\s<([^>]+)>\\s\\.".r
  val propregex = "<([^>]+)>\\s<([^>]+)>\\s(.+)\\.".r
  val propvalregex = "<([^>]+)>\\s(.+)\\.".r

  /**
   * 
   * @param sc SparkContext
   * @param path the path to the file (e.g., /home/data/file or hdfs://file)
   * @param numEdgePartitions the number of partitions for the edge RDD
   * Setting this value to -1 will use the default parallelism.
   * @param edgeStorageLevel the desired storage level for the edge partitions
   * @param vertexStorageLevel the desired storage level for the vertex partitions
   */
  def loadNTriples(
      sc: SparkContext,
      path: String,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[String, String] =
  {
    val startTime = System.currentTimeMillis()

    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    
    val dict = buildDictionary(sc, lines) // dictionary might be too big to collect => try dictionary in RDD / hash ids
    val dictBroadcast = sc.broadcast(dict)
    
    // build vertices and encode RDF properties into the vertex
    val vertices = sc.parallelize(dict.toList).map(pair => {
    	val x = pair._1
    	val y = pair._2
    	x match {
    		case propvalregex(subjprop, value) => (y, value)
    		case _ => (y, x)
    	}
    }).persist(vertexStorageLevel) // TODO: set name
    
    
    val e = sc.parallelize(Array(Edge(1L, 2L, "edgelabel")))
    val edges: RDD[Edge[String]] = lines.map( line => {
    	val dictionary = dictBroadcast.value
    	line match {
    		case relregex(subj, rel, obj)		=> Edge(dictionary.get(subj).get, dictionary.get(obj).get, rel)
    		case propregex(subj, rel, obj)		=> Edge(dictionary.get(subj).get, dictionary.get("<"+subj+"-"+rel+">"+obj).get, rel)
    		case _ => Edge[String](0,0,null)
    	}
    }).persist(edgeStorageLevel) // TODO: set name
    
    val graph = Graph(vertices, edges)
    /*
    	val dictionary = dictBroadcast.value
    	line match {
    		case relregex(subj, rel, obj) 		=> (dictionary.get(subj), (dictionary.get(rel), dictionary.get(obj)))
    		case propregex(subj, prop, value) 	=> (dictionary.get(subj), (dictionary.get(prop), dictionary.get(subj+prop+value)))
    	}
    })
    */
    /*
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            logWarning("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    edges.count()

    logInfo("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
      * */
    return graph // so far
  } // end of edgeListFile
  
  
  
  
  /**
   * Builds a dictionary mapping from Strings (URI's) to Long integer id's
   */
  def buildDictionary(sc: SparkContext, lines: RDD[String]): Map[String, Long] = 
  {
  	val dictaccum = sc.accumulator(Set[String]())(DictionaryAccumulatorParam)
  	lines.foreach(
  			line => {
  		line match {
  			case relregex(subj, rel, obj) 		=>	dictaccum += Set(subj, /*rel, */obj)
  			case propregex(subj, rel, value) 	=> 	dictaccum += Set(subj, /*rel, */"<"+subj+"-"+rel+">"+value)
  			case _ => 
  		}
  	})
  	var map = Map[String, Long]()
  	var counter = 1
  	for (dictitem <- dictaccum.value) {
  		map += (dictitem -> counter)
  		counter += 1
  	}
  	return map
  }

}

object DictionaryAccumulatorParam extends AccumulatorParam[Set[String]] {
	
	def zero(init: Set[String]): Set[String] = {
		return Set[String]()
	}
	
	def addInPlace(a: Set[String], b: Set[String]): Set[String] = {
		return a.union(b)
	}
}
