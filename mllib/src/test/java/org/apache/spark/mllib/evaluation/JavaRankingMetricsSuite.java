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

package org.apache.spark.mllib.evaluation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple2$;
import scala.Tuple3$;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;

public class JavaRankingMetricsSuite extends SharedSparkSession {
  private transient JavaRDD<Tuple2<List<Integer>, List<Integer>>> predictionAndLabels;
  private transient JavaRDD<Tuple3<List<Integer>, List<Integer>, List<Double>>>
    predictionLabelsAndRelevance;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    predictionAndLabels = jsc.parallelize(Arrays.asList(
      Tuple2$.MODULE$.apply(
        Arrays.asList(1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Arrays.asList(1, 2, 3, 4, 5)),
      Tuple2$.MODULE$.apply(
        Arrays.asList(4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Arrays.asList(1, 2, 3)),
      Tuple2$.MODULE$.apply(
        Arrays.asList(1, 2, 3, 4, 5), Arrays.<Integer>asList())), 2);
    predictionLabelsAndRelevance = jsc.parallelize(Arrays.asList(
      Tuple3$.MODULE$.apply(
        Arrays.asList(1, 6, 2, 7, 8, 3, 9, 10, 4, 5),
        Arrays.asList(1, 2, 3, 4, 5),
        Arrays.asList(3.0, 2.0, 1.0, 1.0, 1.0)
      ),
      Tuple3$.MODULE$.apply(
        Arrays.asList(4, 1, 5, 6, 2, 7, 3, 8, 9, 10),
        Arrays.asList(1, 2, 3),
        Arrays.asList(2.0, 0.0, 0.0)
      ),
      Tuple3$.MODULE$.apply(
        Arrays.asList(1, 2, 3, 4, 5),
        Arrays.<Integer>asList(),
        Arrays.<Double>asList()
      )), 3);
  }

  @Test
  public void rankingMetrics() {
    RankingMetrics<?> metrics = RankingMetrics.of(predictionAndLabels);
    Assert.assertEquals(0.355026, metrics.meanAveragePrecision(), 1e-5);
    Assert.assertEquals(0.75 / 3.0, metrics.precisionAt(4), 1e-5);
  }

  @Test
  public void rankingMetricsWithRelevance() {
    RankingMetrics<?> metrics = RankingMetrics.of(predictionLabelsAndRelevance);
    Assert.assertEquals(0.355026, metrics.meanAveragePrecision(), 1e-5);
    Assert.assertEquals(0.511959, metrics.ndcgAt(3), 1e-5);
  }
}
