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

import java.io.Serializable;
import java.util.Arrays;

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
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
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
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testLNormalRDD() {
    double mean = 4.0;
    double std = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = logNormalJavaRDD(sc, mean, std, m);
    JavaDoubleRDD rdd2 = logNormalJavaRDD(sc, mean, std, m, p);
    JavaDoubleRDD rdd3 = logNormalJavaRDD(sc, mean, std, m, p, seed);
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
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
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testExponentialRDD() {
    double mean = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = exponentialJavaRDD(sc, mean, m);
    JavaDoubleRDD rdd2 = exponentialJavaRDD(sc, mean, m, p);
    JavaDoubleRDD rdd3 = exponentialJavaRDD(sc, mean, m, p, seed);
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testGammaRDD() {
    double shape = 1.0;
    double scale = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = gammaJavaRDD(sc, shape, scale, m);
    JavaDoubleRDD rdd2 = gammaJavaRDD(sc, shape, scale, m, p);
    JavaDoubleRDD rdd3 = gammaJavaRDD(sc, shape, scale, m, p, seed);
    for (JavaDoubleRDD rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
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
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
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
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLogNormalVectorRDD() {
    double mean = 4.0;
    double std = 2.0;  
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = logNormalJavaVectorRDD(sc, mean, std, m, n);
    JavaRDD<Vector> rdd2 = logNormalJavaVectorRDD(sc, mean, std, m, n, p);
    JavaRDD<Vector> rdd3 = logNormalJavaVectorRDD(sc, mean, std, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
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
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExponentialVectorRDD() {
    double mean = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = exponentialJavaVectorRDD(sc, mean, m, n);
    JavaRDD<Vector> rdd2 = exponentialJavaVectorRDD(sc, mean, m, n, p);
    JavaRDD<Vector> rdd3 = exponentialJavaVectorRDD(sc, mean, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGammaVectorRDD() {
    double shape = 1.0;
    double scale = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = gammaJavaVectorRDD(sc, shape, scale, m, n);
    JavaRDD<Vector> rdd2 = gammaJavaVectorRDD(sc, shape, scale, m, n, p);
    JavaRDD<Vector> rdd3 = gammaJavaVectorRDD(sc, shape, scale, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testArbitrary() {
    long size = 10;
    long seed = 1L;
    int numPartitions = 0;
    StringGenerator gen = new StringGenerator();
    JavaRDD<String> rdd1 = randomJavaRDD(sc, gen, size);
    JavaRDD<String> rdd2 = randomJavaRDD(sc, gen, size, numPartitions);
    JavaRDD<String> rdd3 = randomJavaRDD(sc, gen, size, numPartitions, seed);
    for (JavaRDD<String> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(size, rdd.count());
      Assert.assertEquals(2, rdd.first().length());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRandomVectorRDD() {
    UniformGenerator generator = new UniformGenerator();
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = randomJavaVectorRDD(sc, generator, m, n);
    JavaRDD<Vector> rdd2 = randomJavaVectorRDD(sc, generator, m, n, p);
    JavaRDD<Vector> rdd3 = randomJavaVectorRDD(sc, generator, m, n, p, seed);
    for (JavaRDD<Vector> rdd: Arrays.asList(rdd1, rdd2, rdd3)) {
      Assert.assertEquals(m, rdd.count());
      Assert.assertEquals(n, rdd.first().size());
    }
  }
}

// This is just a test generator, it always returns a string of 42
class StringGenerator implements RandomDataGenerator<String>, Serializable {
  @Override
  public String nextValue() {
    return "42";
  }
  @Override
  public StringGenerator copy() {
    return new StringGenerator();
  }
  @Override
  public void setSeed(long seed) {
  }
}
