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

package test.org.apache.spark.sql;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;

/**
 * Suite that replicates tests in JavaDatasetAggregatorSuite using lambda syntax.
 */
public class Java8DatasetAggregatorSuite extends JavaDatasetAggregatorSuiteBase {
  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationAverage() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.avg(v -> (double)(v._2() * 2)));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3.0), new Tuple2<>("b", 6.0)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationCount() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.count(v -> v));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 2L), new Tuple2<>("b", 1L)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationSumDouble() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.sum(v -> (double)v._2()));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3.0), new Tuple2<>("b", 3.0)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationSumLong() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.sumLong(v -> (long)v._2()));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3L), new Tuple2<>("b", 3L)),
        aggregated.collectAsList());
  }
}
