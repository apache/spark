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

package org.apache.spark.examples;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */

public class JavaPageRank {
    public static void main(String[] args) {
        if (args.length < 2 ) {
            System.err.println("Usage: JavaPageRank <file> <iter>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaPageRank"));
        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(l -> {
                        String[] arr = l.split("\\s");
                        return new Tuple2(arr[0], arr[1]);
                    }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(v -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs =
                links.join(ranks).values().flatMapToPair(
                    s -> {
                        Iterable<String> url = s._1();
                        Double rank = s._2();
                        int size = Iterables.size(url);

                        List results = new ArrayList();
                        url.forEach(n -> results.add(new Tuple2(n,rank/size)));
                        return results ;
                    }
                );

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey((x,y) -> x+y).mapValues(v -> 0.15+0.85*v);
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach( tup -> System.out.println( tup._1() + " has rank: " + tup._2() + "."));

        sc.stop();

    }
}