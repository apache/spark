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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

public class JavaVertexRDDSuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaVertexRDDSuite");
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  private JavaVertexRDD<Integer> vertexRDD(long n) {
    return vertexRDD(n, 1);
  }

  private JavaVertexRDD<Integer> vertexRDD(long n, int val) {
    return JavaVertexRDD.create(pairRDD(n, val));
  }

  private JavaPairRDD<Long, Integer> pairRDD(long n, int val) {
    List<Tuple2<Long, Integer>> tuples = new ArrayList<Tuple2<Long, Integer>>();
    for (long i = 0; i < n; i++) {
      tuples.add(new Tuple2<Long, Integer>(i, val));
    }
    return sc.parallelizePairs(tuples, 3);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void vertexRDDCreate() {
    List<Tuple2<Long, Integer>> tuples = new ArrayList<Tuple2<Long, Integer>>();
    long n = 100;
    for (long i = 0; i < n; i++) {
      tuples.add(new Tuple2<Long, Integer>(i, 1));
    }

    JavaVertexRDD<Integer> vertexRDD1 = JavaVertexRDD.create(sc.parallelizePairs(tuples));
    Assert.assertEquals(
      new HashSet<Tuple2<Long, Integer>>(tuples),
      new HashSet<Tuple2<Long, Integer>>(vertexRDD1.collect()));

    // Create a graph so we can use its JavaEdgeRDD to construct a JavaVertexRDD
    List<Edge<Integer>> ring = new ArrayList<Edge<Integer>>();
    long m = 200;
    for (long i = 0; i < m; i++) {
      ring.add(new Edge<Integer>(i, (i + 1) % m, 1));
    }
    JavaGraph<Integer, Integer> graph = JavaGraph.fromEdges(sc.parallelize(ring), 1);

    JavaVertexRDD<Integer> vertexRDD2 =
      JavaVertexRDD.create(sc.parallelizePairs(tuples), graph.edges(), 2);
    Assert.assertEquals(m, vertexRDD2.count());

    List<Tuple2<Long, Integer>> duplicateTuples = new ArrayList<Tuple2<Long, Integer>>();
    for (long i = 0; i < n; i++) {
      duplicateTuples.add(new Tuple2<Long, Integer>(i, 1));
      duplicateTuples.add(new Tuple2<Long, Integer>(i, 2));
    }
    JavaGraph<Integer, Integer> emptyGraph =
      JavaGraph.fromEdges(sc.parallelize(new ArrayList<Edge<Integer>>()), 1);

    JavaVertexRDD<Integer> vertexRDD3 = JavaVertexRDD.create(
      sc.parallelizePairs(duplicateTuples), emptyGraph.edges(), 2,
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
      });
    Assert.assertEquals(n, vertexRDD3.count());
    for (Tuple2<Long, Integer> kv : vertexRDD3.collect()) {
      Assert.assertEquals(3, kv._2().intValue());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void vertexRDDFromEdges() {
    // Create a graph so we can use its JavaEdgeRDD to construct a JavaVertexRDD
    List<Edge<Integer>> ring = new ArrayList<Edge<Integer>>();
    long m = 200;
    for (long i = 0; i < m; i++) {
      ring.add(new Edge<Integer>(i, (i + 1) % m, 1));
    }
    JavaGraph<Integer, Integer> graph = JavaGraph.fromEdges(sc.parallelize(ring), 1);

    JavaVertexRDD<Integer> vertexRDD = JavaVertexRDD.fromEdges(graph.edges(), 1, 1);
    Assert.assertEquals(m, vertexRDD.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapValues() {
    vertexRDD(100).mapValues(
      new Function<Integer, Integer>() {
        public Integer call(Integer v) {
          return v * 2;
        }
      }).collect();

    vertexRDD(100).mapValues(
      new Function2<Long, Integer, Long>() {
        public Long call(Long id, Integer v) {
          return id + v * 2;
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void diff() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaVertexRDD<Integer> b = vertexRDD(100, 2);
    a.diff(b).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void leftZipJoin() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaVertexRDD<Integer> b = vertexRDD(100, 2);
    a.leftZipJoin(b,
      new Function3<Long, Integer, Optional<Integer>, Integer>() {
        public Integer call(Long id, Integer a, Optional<Integer> bOpt) {
          return a + bOpt.or(0);
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void leftJoin() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaPairRDD<Long, Integer> b = pairRDD(100, 2);
    a.leftJoin(b,
      new Function3<Long, Integer, Optional<Integer>, Integer>() {
        public Integer call(Long id, Integer a, Optional<Integer> bOpt) {
          return a + bOpt.or(0);
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void innerZipJoin() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaVertexRDD<Integer> b = vertexRDD(100, 2);
    a.innerZipJoin(b,
      new Function3<Long, Integer, Integer, Integer>() {
        public Integer call(Long id, Integer a, Integer b) {
          return a + b;
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void innerJoin() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaPairRDD<Long, Integer> b = pairRDD(100, 2);
    a.innerJoin(b,
      new Function3<Long, Integer, Integer, Integer>() {
        public Integer call(Long id, Integer a, Integer b) {
          return a + b;
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void aggregateUsingIndex() {
    JavaVertexRDD<Integer> a = vertexRDD(100, 1);
    JavaPairRDD<Long, Integer> b = pairRDD(100, 2);
    a.aggregateUsingIndex(b,
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
      }).collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void withEdges() {
    // Create a graph so we can use its JavaEdgeRDD
    List<Edge<Integer>> ring = new ArrayList<Edge<Integer>>();
    long m = 200;
    for (long i = 0; i < m; i++) {
      ring.add(new Edge<Integer>(i, (i + 1) % m, 1));
    }
    JavaGraph<Integer, Integer> graph = JavaGraph.fromEdges(sc.parallelize(ring), 1);

    JavaVertexRDD<Integer> vertexRDD = vertexRDD(100, 1);
    vertexRDD.withEdges(graph.edges()).collect();
  }
}
