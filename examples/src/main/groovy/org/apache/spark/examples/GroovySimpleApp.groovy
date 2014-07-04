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

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf


class GroovySimpleApp {
    static def main(args) {
        // Should be some file on your system
        def logFile = "README.md"
        def conf = new SparkConf().setAppName("Simple Groovy Application")
        def sc = new JavaSparkContext(conf)
        def logData = sc.textFile(logFile).cache()

        def numAs = logData.filter({it.contains("a")}).count()

        def numBs = logData.filter({it.contains("b")}).count()

        println("Lines with a: $numAs, lines with b: $numBs")
    }
}