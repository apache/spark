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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import static org.apache.spark.mllib.random.RandomRDDs.*;

public class JavaRandomRDDsSuite extends SharedSparkSession {

  @Test
  public void testUniformRDD() {
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = uniformJavaRDD(jsc, m);
    JavaDoubleRDD rdd2 = uniformJavaRDD(jsc, m, p);
    JavaDoubleRDD rdd3 = uniformJavaRDD(jsc, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testNormalRDD() {
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = normalJavaRDD(jsc, m);
    JavaDoubleRDD rdd2 = normalJavaRDD(jsc, m, p);
    JavaDoubleRDD rdd3 = normalJavaRDD(jsc, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testLNormalRDD() {
    double mean = 4.0;
    double std = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = logNormalJavaRDD(jsc, mean, std, m);
    JavaDoubleRDD rdd2 = logNormalJavaRDD(jsc, mean, std, m, p);
    JavaDoubleRDD rdd3 = logNormalJavaRDD(jsc, mean, std, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testPoissonRDD() {
    double mean = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = poissonJavaRDD(jsc, mean, m);
    JavaDoubleRDD rdd2 = poissonJavaRDD(jsc, mean, m, p);
    JavaDoubleRDD rdd3 = poissonJavaRDD(jsc, mean, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testExponentialRDD() {
    double mean = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = exponentialJavaRDD(jsc, mean, m);
    JavaDoubleRDD rdd2 = exponentialJavaRDD(jsc, mean, m, p);
    JavaDoubleRDD rdd3 = exponentialJavaRDD(jsc, mean, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }

  @Test
  public void testGammaRDD() {
    double shape = 1.0;
    double jscale = 2.0;
    long m = 1000L;
    int p = 2;
    long seed = 1L;
    JavaDoubleRDD rdd1 = gammaJavaRDD(jsc, shape, jscale, m);
    JavaDoubleRDD rdd2 = gammaJavaRDD(jsc, shape, jscale, m, p);
    JavaDoubleRDD rdd3 = gammaJavaRDD(jsc, shape, jscale, m, p, seed);
    for (JavaDoubleRDD rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
    }
  }


  @Test
  public void testUniformVectorRDD() {
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = uniformJavaVectorRDD(jsc, m, n);
    JavaRDD<Vector> rdd2 = uniformJavaVectorRDD(jsc, m, n, p);
    JavaRDD<Vector> rdd3 = uniformJavaVectorRDD(jsc, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testNormalVectorRDD() {
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = normalJavaVectorRDD(jsc, m, n);
    JavaRDD<Vector> rdd2 = normalJavaVectorRDD(jsc, m, n, p);
    JavaRDD<Vector> rdd3 = normalJavaVectorRDD(jsc, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testLogNormalVectorRDD() {
    double mean = 4.0;
    double std = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = logNormalJavaVectorRDD(jsc, mean, std, m, n);
    JavaRDD<Vector> rdd2 = logNormalJavaVectorRDD(jsc, mean, std, m, n, p);
    JavaRDD<Vector> rdd3 = logNormalJavaVectorRDD(jsc, mean, std, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testPoissonVectorRDD() {
    double mean = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = poissonJavaVectorRDD(jsc, mean, m, n);
    JavaRDD<Vector> rdd2 = poissonJavaVectorRDD(jsc, mean, m, n, p);
    JavaRDD<Vector> rdd3 = poissonJavaVectorRDD(jsc, mean, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testExponentialVectorRDD() {
    double mean = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = exponentialJavaVectorRDD(jsc, mean, m, n);
    JavaRDD<Vector> rdd2 = exponentialJavaVectorRDD(jsc, mean, m, n, p);
    JavaRDD<Vector> rdd3 = exponentialJavaVectorRDD(jsc, mean, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testGammaVectorRDD() {
    double shape = 1.0;
    double jscale = 2.0;
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = gammaJavaVectorRDD(jsc, shape, jscale, m, n);
    JavaRDD<Vector> rdd2 = gammaJavaVectorRDD(jsc, shape, jscale, m, n, p);
    JavaRDD<Vector> rdd3 = gammaJavaVectorRDD(jsc, shape, jscale, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
    }
  }

  @Test
  public void testArbitrary() {
    long size = 10;
    long seed = 1L;
    int numPartitions = 0;
    StringGenerator gen = new StringGenerator();
    JavaRDD<String> rdd1 = randomJavaRDD(jsc, gen, size);
    JavaRDD<String> rdd2 = randomJavaRDD(jsc, gen, size, numPartitions);
    JavaRDD<String> rdd3 = randomJavaRDD(jsc, gen, size, numPartitions, seed);
    for (JavaRDD<String> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(size, rdd.count());
      Assertions.assertEquals(2, rdd.first().length());
    }
  }

  @Test
  public void testRandomVectorRDD() {
    UniformGenerator generator = new UniformGenerator();
    long m = 100L;
    int n = 10;
    int p = 2;
    long seed = 1L;
    JavaRDD<Vector> rdd1 = randomJavaVectorRDD(jsc, generator, m, n);
    JavaRDD<Vector> rdd2 = randomJavaVectorRDD(jsc, generator, m, n, p);
    JavaRDD<Vector> rdd3 = randomJavaVectorRDD(jsc, generator, m, n, p, seed);
    for (JavaRDD<Vector> rdd : Arrays.asList(rdd1, rdd2, rdd3)) {
      Assertions.assertEquals(m, rdd.count());
      Assertions.assertEquals(n, rdd.first().size());
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
