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

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


/**
 * Computes an approximation to pi
 * Usage: GroovySparkPi [slices].
 */
class GroovySparkPi {

    static def main(args){

        def sparkConf = new SparkConf().setAppName("GroovySparkPi")
        def jsc = new JavaSparkContext(sparkConf)

        def slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2
        def n = 100000 * slices
        def l = 0..n
        def dataSet = jsc.parallelize(l, slices)
        def count = dataSet.map({e ->
                double x = Math.random() * 2 - 1
                double y = Math.random() * 2 - 1
                (x * x + y * y < 1) ? 1 : 0
            }).reduce({i1, i2 -> i1 + i2})

        println("Pi is roughly " + 4.0 * count / n)

        jsc.stop()

    }

}
