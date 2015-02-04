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

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._
import scala.util._

object SparkAPSP {
  def main(args: Array[String]){
     if(args.length < 1){
     System.err.println("Usage: SparkAPSP <file>")
     System.exit(1)
     }
     val conf = new SparkConf().setAppName("Diameter Estimation")
     val sc = new SparkContext(conf)
     val lines = sc.textFile(args(0))
     var edges = lines.map(s =>{  
     val field = s.split("\\s+")
     (field(0).toLong,field(1).toLong)
     }).distinct().cache()
     
     var distances = edges.map(pair => (pair, 1)).cache()
     var prevDistsSize = 0L
     var distsSize = distances.count()
     
     while (prevDistsSize < distsSize) {
     val newDists = distances.map {case ((a, b), dist) => (b, (a, dist))}.join(edges)
     .map {case (b, ((a, dist), c)) => ((a, c), dist + 1)}.cache() 

     distances = distances.union(newDists).reduceByKey((a, b) => math.min(a,b)).cache()
     prevDistsSize = distsSize 
     distsSize = distances.count() 
     }
     val finalres = distances.collect()
     finalres.foreach(res => println(res._1 + " -> distance " + res._2))
}	
}
