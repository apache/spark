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

package org.apache.spark.mllib.random;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import static org.apache.spark.mllib.random.RandomRDDs.*;

public class JavaRandomRDDsSuite {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaRandomRDDsSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void testUniformRDD() {
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = uniformJavaRDD(sc, m);
    JavaDoubleRDD rdd2 = uniformJavaRDD(sc, m, p);
    JavaDoubleRDD rdd3 = uniformJavaRDD(sc, m, p, seed);
    for (JavaDoubleRDD rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testNormalRDD() {
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = normalJavaRDD(sc, m);
    JavaDoubleRDD rdd2 = normalJavaRDD(sc, m, p);
    JavaDoubleRDD rdd3 = normalJavaRDD(sc, m, p, seed);
    for (JavaDoubleRDD rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testPoissonRDD() {
    double mean = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = poissonJavaRDD(sc, mean, m);
    JavaDoubleRDD rdd2 = poissonJavaRDD(sc, mean, m, p);
    JavaDoubleRDD rdd3 = poissonJavaRDD(sc, mean, m, p, seed);
    for (JavaDoubleRDD rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUniformVectorRDD() {
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = uniformJavaVectorRDD(sc, m, n);
    JavaRDD<Vector> rdd2 = uniformJavaVectorRDD(sc, m, n, p);
    JavaRDD<Vector> rdd3 = uniformJavaVectorRDD(sc, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNormalVectorRDD() {
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = normalJavaVectorRDD(sc, m, n);
    JavaRDD<Vector> rdd2 = normalJavaVectorRDD(sc, m, n, p);
    JavaRDD<Vector> rdd3 = normalJavaVectorRDD(sc, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPoissonVectorRDD() {
    double mean = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = poissonJavaVectorRDD(sc, mean, m, n);
    JavaRDD<Vector> rdd2 = poissonJavaVectorRDD(sc, mean, m, n, p);
    JavaRDD<Vector> rdd3 = poissonJavaVectorRDD(sc, mean, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Lists.newArrayList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }
}
