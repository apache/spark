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

import java.io.*;
import java.net.URI;
import java.util.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Optional;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;

import org.apache.spark.graphx.*;
import org.apache.spark.graphx.api.java.*;

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
