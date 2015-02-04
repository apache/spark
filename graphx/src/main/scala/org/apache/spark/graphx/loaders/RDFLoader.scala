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
import java.security.MessageDigest

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
    
    val vertices = lines.flatMap(line => {
      line match {
        case relregex(subj, rel, obj)     => Set(subj, obj)
        case propregex(subj, rel, value)  => Set(subj, "<" + subj + "-" + rel + ">" + value)
      }
    }).distinct().map(name => 
      name match {
        case propvalregex(pre, value) => (gethash("<" + pre + ">" + value), value)
        case _ => (gethash(name), name)
      }
    ).persist(vertexStorageLevel) // TODO: set name etc    
    
    val edges: RDD[Edge[String]] = lines.map( line => {
      line match {
        case relregex(subj, rel, obj)     => Edge(gethash(subj), gethash(obj), rel)
        case propregex(subj, rel, obj)    
            => Edge(gethash(subj), gethash("<" + subj + "-" + rel + ">" + obj), rel)
      }
    }).persist(edgeStorageLevel) // TODO: set name
    
    val graph = Graph(vertices, edges)
    return graph // so far
  } // end of edgeListFile
  
  def gethash(in:String):Long = {
    val hasher = MessageDigest.getInstance("md5")
    val hash = hasher.digest(in.getBytes())
    hash.map(x => (x.toInt + 128).toString).reduce((x, y) => x + y).toLong
  }

}
