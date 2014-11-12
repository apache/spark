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

package org.apache.spark.graphx;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.api.java.*;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple4;

public class JavaGraphSuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  private JavaGraph<String, Integer> starGraph(long n) {
    List<Tuple2<Long, Long>> edges = new ArrayList<Tuple2<Long, Long>>();
    for (long i = 1; i < n; i++) {
      edges.add(new Tuple2<Long, Long>(0L, i));
    }
    return JavaGraph.fromEdgeTuples(sc.parallelizePairs(edges, 3), "v");
  }

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaGraphSuite");
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void graphFromEdgeTuples() {
    List<Tuple2<Long, Long>> doubleRing = new ArrayList<Tuple2<Long, Long>>();
    long n = 100;
    for (long i = 0; i < n; i++) {
      doubleRing.add(new Tuple2<Long, Long>(i, (i + 1) % n));
      doubleRing.add(new Tuple2<Long, Long>(i, (i + 1) % n));
    }

    JavaGraph<Integer, Integer> graph =
      JavaGraph.fromEdgeTuples(sc.parallelizePairs(doubleRing), 1);
    Assert.assertEquals(doubleRing.size(), graph.edges().count());
    for (Edge<Integer> e : graph.edges().collect()) {
      Assert.assertEquals(1, e.attr.longValue());
    }

    // uniqueEdges option should uniquify edges and store duplicate count in edge attributes
    JavaGraph<Integer, Integer> uniqueGraph = JavaGraph.fromEdgeTuples(
      sc.parallelizePairs(doubleRing), 1, PartitionStrategies.RandomVertexCut,
      StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY());
    Assert.assertEquals(n, uniqueGraph.edges().count());
    for (Edge<Integer> e : uniqueGraph.edges().collect()) {
      Assert.assertEquals(2, e.attr.longValue());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void graphFromEdges() {
    List<Edge<Integer>> ring = new ArrayList<Edge<Integer>>();
    long n = 100;
    for (long i = 0; i < n; i++) {
      ring.add(new Edge<Integer>(i, (i + 1) % n, 1));
    }

    JavaGraph<Float, Integer> graph = JavaGraph.fromEdges(sc.parallelize(ring), 1.0f);
    Assert.assertEquals(ring.size(), graph.edges().count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void graphCreate() {
    List<Edge<Integer>> edges = new ArrayList<Edge<Integer>>();
    long n = 100;
    for (long i = 0; i < n; i++) {
      edges.add(new Edge<Integer>(i, (i + 1) % n, 1));
    }

    List<Tuple2<Long, Boolean>> vertices = new ArrayList<Tuple2<Long, Boolean>>();
    for (long i = 0; i < 10; i++) {
      vertices.add(new Tuple2<Long, Boolean>(i, true));
    }

    JavaGraph<Boolean, Integer> graph =
      JavaGraph.create(sc.parallelizePairs(vertices), sc.parallelize(edges), false);

    Assert.assertEquals(edges.size(), graph.edges().count());

    // Vertices not explicitly provided but referenced by edges should be created automatically
    Assert.assertEquals(100, graph.vertices().count());

    for (EdgeTriplet<Boolean, Integer> et : graph.triplets().collect()) {
      Assert.assertTrue((et.srcId() < 10 && et.srcAttr()) || (et.srcId() >= 10 && !et.srcAttr()));
      Assert.assertTrue((et.dstId() < 10 && et.dstAttr()) || (et.dstId() >= 10 && !et.dstAttr()));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void triplets() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    JavaRDD<Tuple4<Long, Long, String, String>> triplets = star.triplets().map(
      new Function<EdgeTriplet<String, Integer>, Tuple4<Long, Long, String, String>>() {
        public Tuple4<Long, Long, String, String> call(EdgeTriplet<String, Integer> et) {
          return new Tuple4<Long, Long, String, String>(
            et.srcId(), et.dstId(), et.srcAttr(), et.dstAttr());
        }
      });

    Set<Tuple4<Long, Long, String, String>> tripletsExpected =
      new HashSet<Tuple4<Long, Long, String, String>>();
    for (long i = 1; i < n; i++) {
      tripletsExpected.add(new Tuple4<Long, Long, String, String>(0L, i, "v", "v"));
    }
    Assert.assertEquals(
      tripletsExpected, new HashSet<Tuple4<Long, Long, String, String>>(triplets.collect()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void partitionBy() {
    JavaGraph<String, Integer> star = starGraph(10);
    JavaGraph<String, Integer> star2D = star.partitionBy(PartitionStrategies.EdgePartition2D);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapVertices() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.mapVertices(
      new Function2<Long, String, String>() {
        public String call(Long id, String attr) {
          return attr + id;
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapEdges() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.mapEdges(
      new Function<Edge<Integer>, String>() {
        public String call(Edge<Integer> e) {
          return e.attr().toString();
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapTriplets() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.mapTriplets(
      new Function<EdgeTriplet<String, Integer>, String>() {
        public String call(EdgeTriplet<String, Integer> et) {
          return et.srcAttr() + et.dstAttr();
        }
      }, TripletFields.SrcDstOnly);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void reverse() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.reverse();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void subgraph() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.subgraph(
      new Function<EdgeTriplet<String, Integer>, Boolean>() {
        public Boolean call(EdgeTriplet<String, Integer> et) {
          return et.attr() == 1;
        }
      },
      new Function2<Long, String, Boolean>() {
        public Boolean call(Long id, String attr) {
          return id > 3;
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mask() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    JavaGraph<String, Integer> star2 = starGraph(n + 5);
    star2.mask(star);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void groupEdges() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.groupEdges(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void aggregateMessages() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    VoidFunction<EdgeContext<String, Integer, String>> sendMsg =
      new VoidFunction<EdgeContext<String, Integer, String>>() {
        public void call(EdgeContext<String, Integer, String> ctx) {
          ctx.sendToDst(ctx.srcAttr());
        }
      };
    Function2<String, String, String> mergeMsg =
      new Function2<String, String, String>() {
        public String call(String a, String b) {
          return a + b;
        }
      };
    star.aggregateMessages(sendMsg, mergeMsg, TripletFields.SrcOnly);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void outerJoinVertices() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.outerJoinVertices(
      star.vertices(),
      new Function3<Long, String, Optional<String>, String>() {
        public String call(Long id, String a, Optional<String> bOpt) {
          return a + bOpt.or("");
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void simpleOps() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.numEdges();
    star.numVertices();
    star.inDegrees();
    star.outDegrees();
    star.degrees();
    star.collectNeighborIds(EdgeDirection.Out());
    star.collectNeighbors(EdgeDirection.Out());
    star.collectEdges(EdgeDirection.Out());
    star.pickRandomVertex();
    star.pageRank(0.01, 0.15);
    star.staticPageRank(10, 0.15);
    star.connectedComponents();
    star.triangleCount();
    star.stronglyConnectedComponents(10);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void joinVertices() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.joinVertices(
      star.vertices(),
      new Function3<Long, String, String, String>() {
        public String call(Long id, String a, String b) {
          return a + b;
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void filter() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.filter(
      new Function<JavaGraph<String, Integer>, JavaGraph<String, Integer>>() {
        public JavaGraph<String, Integer> call(JavaGraph<String, Integer> graph) {
          return graph;
        }
      },
      new Function<EdgeTriplet<String, Integer>, Boolean>() {
        public Boolean call(EdgeTriplet<String, Integer> et) {
          return et.attr() == 1;
        }
      },
      new Function2<Long, String, Boolean>() {
        public Boolean call(Long id, String attr) {
          return id > 3;
        }
      });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void pregel() {
    long n = 5;
    JavaGraph<String, Integer> star = starGraph(n);
    star.pregel(
      "", 10, EdgeDirection.Either(),
      new Function3<Long, String, String, String>() {
        public String call(Long id, String attr, String msg) {
          return attr + msg;
        }
      },
      new PairFlatMapFunction<EdgeTriplet<String, Integer>, Long, String>() {
        public Iterable<Tuple2<Long, String>> call(EdgeTriplet<String, Integer> et) {
          List<Tuple2<Long, String>> msgs = new ArrayList<Tuple2<Long, String>>();
          msgs.add(new Tuple2<Long, String>(et.dstId(), et.srcAttr()));
          return msgs;
        }
      },
      new Function2<String, String, String>() {
        public String call(String a, String b) {
          return a + b;
        }
      });
  }
}
