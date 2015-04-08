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

import com.google.common.io.Files;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.api.java.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JavaEdgeRDDSuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaEdgeRDDSuite");
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  private JavaEdgeRDD<String, Integer> edgeRDD(long n) {
    List<Edge<String>> edges = new ArrayList<Edge<String>>();
    for (long i = 0; i < n; i++) {
      edges.add(new Edge<String>(i, (i + 1) % n, "e"));
    }
    return JavaEdgeRDD.fromEdges(sc.parallelize(edges, 3));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void edgeRDDFromEdges() {
    long n = 100;
    JavaEdgeRDD<String, Integer> edges = edgeRDD(n);

    Assert.assertEquals(n, edges.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapValues() {
    long n = 100;
    JavaEdgeRDD<String, Integer> edges = edgeRDD(n);
    JavaEdgeRDD<Integer, Integer> mapped = edges.mapValues(
      new Function<Edge<String>, Integer>() {
        public Integer call(Edge<String> e) {
          return (int)e.srcId();
        }
      });
    Assert.assertEquals(n, mapped.count());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void reverse() {
    long n = 100;
    JavaEdgeRDD<String, Integer> edges = edgeRDD(n);
    Assert.assertEquals(n, edges.reverse().count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void filter() {
    long n = 100;
    JavaEdgeRDD<String, Integer> edges = edgeRDD(n);
    JavaEdgeRDD<String, Integer> filtered = edges.filter(
      new Function<EdgeTriplet<Integer, String>, Boolean>() {
        public Boolean call(EdgeTriplet<Integer, String> e) {
          return e.attr().equals("e");
        }
      },
      new Function2<Long, Integer, Boolean>() {
        public Boolean call(Long id, Integer attr) {
          return id < 10;
        }
      });
    Assert.assertEquals(9, filtered.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void innerJoin() {
    long n = 100;
    JavaEdgeRDD<String, Integer> a = edgeRDD(n);
    JavaEdgeRDD<String, Integer> b = a.filter(
      new Function<EdgeTriplet<Integer, String>, Boolean>() {
        public Boolean call(EdgeTriplet<Integer, String> e) {
          return true;
        }
      },
      new Function2<Long, Integer, Boolean>() {
        public Boolean call(Long id, Integer attr) {
          return id < 10;
        }
      });

    JavaEdgeRDD<String, Integer> joined = a.innerJoin(
      b,
      new Function4<Long, Long, String, String, String>() {
        public String call(Long src, Long dst, String a, String b) {
          return a + b;
        }
      });

    for (Edge<String> e : joined.collect()) {
      Assert.assertEquals("ee", e.attr());
    }
    Assert.assertEquals(b.count(), joined.count());
  }
}
