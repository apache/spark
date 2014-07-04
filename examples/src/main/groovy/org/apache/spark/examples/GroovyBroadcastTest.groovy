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
 *
 * Author: Constantin Ahlmann-Eltze
 * Affiliation: Parallel and Distributed Systems Group, Heidelberg
 * University, http://pvs.ifi.uni-heidelberg.de/home/
 * Date: July 4th, 2014
 */

package org.apache.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

class GroovyBroadcastTest {

    static void main(String[] args){

        def conf = new SparkConf().setAppName("GroovyBroadcastTest")
        def sc = new JavaSparkContext(conf)
        def slices = (args.size() > 1)  ? args[0].toInteger() : 2
        def num = (args.size() > 2)  ? args[1].toInteger() : 1000000

        def arr1 = 0..num-1

        for(i in 0..3){
            println("Iteration " + i)
            println("===========")
            def barr1 = sc.broadcast(arr1)
            sc.parallelize(1..10, slices).foreach({
                println(barr1.value().size())
            })
        }
        sc.stop()
    }

}
