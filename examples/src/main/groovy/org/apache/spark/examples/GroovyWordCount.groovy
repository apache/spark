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

import java.util.regex.Pattern
import scala.Tuple2
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


/**
 * Counts the words in a file
 * Usage: GroovyWordCount <File name>.
 */
class GroovyWordCount {

    private static final def SPACE = Pattern.compile(" ")

    static def main(args){

        if(args.length < 1){
            System.err.println("Usage GroovyWordCount <file>")
            System.exit(1)
        }
        def sparkConf = new SparkConf().setAppName("GroovyWordCount")
        def sc = new JavaSparkContext(sparkConf)
        def lines = sc.textFile(args[0], 1)
        def words = lines.flatMap({ s -> Arrays.asList(SPACE.split(s))})
        def counts = words.mapToPair({ new Tuple2(it, 1) }).reduceByKey({ a, b -> a + b })
        println("Here are 5 words with there count in the file")
        counts.take(5).each({t ->  println(t)})
        sc.stop()

    }


}
