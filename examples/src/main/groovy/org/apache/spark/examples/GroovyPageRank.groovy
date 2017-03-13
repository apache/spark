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
import com.google.common.collect.Iterables
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
class GroovyPageRank {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    static def main(args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>")
            System.exit(1)
        }

        def sparkConf = new SparkConf().setAppName("JavaPageRank")
        def sc = new JavaSparkContext(sparkConf)

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        def lines = sc.textFile(args[0], 1)

        // Loads all URLs from input file and initialize their neighbors.
        def links = lines.mapToPair({s ->
                String[] parts = SPACES.split(s)
                new Tuple2(parts[0], parts[1])
        }).distinct().groupByKey().cache()

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        def ranks = links.mapValues({1})

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            def contribs = links.join(ranks).values().flatMapToPair({s ->
                    def urlCount = Iterables.size(s._1)
                    def results = new ArrayList<Tuple2<String, Double>>()
//                    for (String n : s._1) {
                    s._1.each({n -> results.add(new Tuple2(n, s._2() / urlCount))})
                    return results
            })

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey({d1, d2 -> d1+d2}).mapValues({sum -> 0.15 + sum * 0.85})
        }

        // Collects all URL ranks and dump them to console.
        def output = ranks.collect();
        output.each({tuple -> println(tuple._1() + " has rank: " + tuple._2() + ".")})

        sc.stop();
    }

}
