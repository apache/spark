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


class GroovyGroupByTest {

    static def main(args){

        def numMappers = (args.length > 0) ? args[0].toInteger() : 2
        def numKVPairs = (args.length > 1) ? args[1].toInteger() : 1000
        def valSize = (args.length > 2) ? args[2].toInteger() : 1000
        def numReducers = (args.length > 3) ? args[3].toInteger() : numMappers

        def conf = new SparkConf().setAppName("Groovy GroupBy Test")
        def sc = new JavaSparkContext(conf)

        def pairs1 = sc.parallelize(0..numMappers-1, numMappers).flatMapToPair( {
            def ranGen = new Random()
            def arr1 = new Tuple2[numKVPairs]
            for (i in 0..numKVPairs-1) {
                def byteArr = new Byte[valSize]
                ranGen.nextBytes(byteArr)
                arr1[i] = new Tuple2(ranGen.nextInt(Integer.MAX_VALUE), byteArr)
            }
            Arrays.asList(arr1)
        }).cache()

        // Enforce that everything has been calculated and in cache
        pairs1.count()

        println(pairs1.groupByKey(numReducers).count())

        sc.stop()
    }

}
