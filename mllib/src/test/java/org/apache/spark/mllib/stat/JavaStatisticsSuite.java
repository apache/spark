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

package org.apache.spark.mllib.stat;

import java.io.Serializable;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;

public class JavaStatisticsSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaStatistics");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void testCorr() {
    JavaRDD<Double> x = sc.parallelize(Lists.newArrayList(1.0, 2.0, 3.0, 4.0));
    JavaRDD<Double> y = sc.parallelize(Lists.newArrayList(1.1, 2.2, 3.1, 4.3));

    Double corr1 = Statistics.corr(x, y);
    Double corr2 = Statistics.corr(x, y, "pearson");
    // Check default method
    assertEquals(corr1, corr2);
  }

  @Test
  public void kolmogorovSmirnovTest() {
    JavaDoubleRDD data = sc.parallelizeDoubles(Lists.newArrayList(0.2, 1.0, -1.0, 2.0));
    KolmogorovSmirnovTestResult testResult1 = Statistics.kolmogorovSmirnovTest(data, "norm");
    KolmogorovSmirnovTestResult testResult2 = Statistics.kolmogorovSmirnovTest(
      data, "norm", 0.0, 1.0);
  }

  @Test
  public void chiSqTest() {
    JavaRDD<LabeledPoint> data = sc.parallelize(Lists.newArrayList(
      new LabeledPoint(0.0, Vectors.dense(0.1, 2.3)),
      new LabeledPoint(1.0, Vectors.dense(1.5, 5.1)),
      new LabeledPoint(0.0, Vectors.dense(2.4, 8.1))));
    ChiSqTestResult[] testResults = Statistics.chiSqTest(data);
  }
}
