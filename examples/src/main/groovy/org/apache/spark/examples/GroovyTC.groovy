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
 *
 * Author: Constantin Ahlmann-Eltze
 * Affiliation: Parallel and Distributed Systems Group, Heidelberg
 * University, http://pvs.ifi.uni-heidelberg.de/home/
 * Date: July 4th, 2014
 */

package org.apache.spark.examples

import scala.Tuple2
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


/**
 * Transitive closure on a graph, implemented in Java.
 * Usage: GroovyTC [slices]
 */
class GroovyTC {

    static def rand = new Random(42)
    static def numEdges = 20
    static def numVertices = 100

    static def generateGraph(){
        def edges = new HashSet(numEdges)
        while(edges.size() < numEdges){
            def from = rand.nextInt(numVertices)
            def to = rand.nextInt(numVertices)
            def e = new Tuple2(from, to)
            if(from != to)
                edges.add(e)
        }
        return edges.toList()
    }

    static final def projectFN = {new Tuple2(it._2._2, it._2._1)}

    static def main(args){
        def sparkConf = new SparkConf().setAppName("GroovyHdfsLR")
        def sc = new JavaSparkContext(sparkConf)
        def slices = (args.length > 1) ? args[0].toInteger() : 2
        def tc = sc.parallelizePairs(generateGraph(), slices).cache()

        def edges = tc.mapToPair({new Tuple2(it._2, it._1)})
        def oldCount = 0L
        def nextCount = tc.count()
        while(nextCount != oldCount){
            oldCount = nextCount
            tc = tc.union(tc.join(edges).mapToPair(projectFN))
            tc = tc.distinct().cache()
            nextCount = tc.count()
        }
        println("TC has ${tc.count()} edges.")
        System.exit(0)
    }

}
