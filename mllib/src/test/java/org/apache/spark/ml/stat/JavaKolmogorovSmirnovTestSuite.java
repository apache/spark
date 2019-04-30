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

package org.apache.spark.ml.stat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.spark.sql.Encoders;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class JavaKolmogorovSmirnovTestSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    List<java.lang.Double> points = Arrays.asList(0.1, 1.1, 10.1, -1.1);

    dataset = spark.createDataset(points, Encoders.DOUBLE()).toDF("sample");
  }

  @Test
  public void testKSTestCDF() {
    // Create theoretical distributions
    NormalDistribution stdNormalDist = new NormalDistribution(0, 1);

    // set seeds
    Long seed = 10L;
    stdNormalDist.reseedRandomGenerator(seed);
    Function<Double, Double> stdNormalCDF = (x) -> stdNormalDist.cumulativeProbability(x);

    double pThreshold = 0.05;

    // Comparing a standard normal sample to a standard normal distribution
    Row results = KolmogorovSmirnovTest
      .test(dataset, "sample", stdNormalCDF).head();
    double pValue1 = results.getDouble(0);
    // Cannot reject null hypothesis
    assert(pValue1 > pThreshold);
  }

  @Test
  public void testKSTestNamedDistribution() {
    double pThreshold = 0.05;

    // Comparing a standard normal sample to a standard normal distribution
    Row results = KolmogorovSmirnovTest
            .test(dataset, "sample", "norm", 0.0, 1.0).head();
    double pValue1 = results.getDouble(0);
    // Cannot reject null hypothesis
    assert(pValue1 > pThreshold);
  }
}
