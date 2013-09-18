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

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Transitive closure on a graph, implemented in Java.
 */
public class JavaTC {

  static int numEdges = 200;
  static int numVertices = 100;
  static Random rand = new Random(42);

  static List<Tuple2<Integer, Integer>> generateGraph() {
    Set<Tuple2<Integer, Integer>> edges = new HashSet<Tuple2<Integer, Integer>>(numEdges);
    while (edges.size() < numEdges) {
      int from = rand.nextInt(numVertices);
      int to = rand.nextInt(numVertices);
      Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(from, to);
      if (from != to) edges.add(e);
    }
    return new ArrayList<Tuple2<Integer, Integer>>(edges);
  }

  static class ProjectFn extends PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>,
      Integer, Integer> {
    static ProjectFn INSTANCE = new ProjectFn();

    public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
      return new Tuple2<Integer, Integer>(triple._2()._2(), triple._2()._1());
    }
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("Usage: JavaTC <host> [<slices>]");
      System.exit(1);
    }

    JavaSparkContext sc = new JavaSparkContext(args[0], "JavaTC",
        System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));
    Integer slices = (args.length > 1) ? Integer.parseInt(args[1]): 2;
    JavaPairRDD<Integer, Integer> tc = sc.parallelizePairs(generateGraph(), slices).cache();

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    JavaPairRDD<Integer, Integer> edges = tc.map(
      new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
        public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> e) {
          return new Tuple2<Integer, Integer>(e._2(), e._1());
        }
    });

    long oldCount = 0;
    long nextCount = tc.count();
    do {
      oldCount = nextCount;
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(ProjectFn.INSTANCE)).distinct().cache();
      nextCount = tc.count();
    } while (nextCount != oldCount);

    System.out.println("TC has " + tc.count() + " edges.");
    System.exit(0);
  }
}
