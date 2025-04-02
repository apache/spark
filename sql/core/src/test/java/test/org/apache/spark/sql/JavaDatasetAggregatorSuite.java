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

import scala.Tuple2;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * Suite for testing the aggregate functionality of Datasets in Java.
 */
public class JavaDatasetAggregatorSuite extends JavaDatasetAggregatorSuiteBase {
  @Test
  public void testTypedAggregationAnonClass() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();

    Dataset<Tuple2<String, Integer>> aggregated = grouped.agg(new IntSumOf().toColumn());
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3), new Tuple2<>("b", 3)),
        aggregated.collectAsList());

    Dataset<Tuple2<String, Integer>> aggregated2 = grouped.agg(new IntSumOf().toColumn())
      .as(Encoders.tuple(Encoders.STRING(), Encoders.INT()));
    Assertions.assertEquals(
      Arrays.asList(
        new Tuple2<>("a", 3),
        new Tuple2<>("b", 3)),
      aggregated2.collectAsList());
  }

  static class IntSumOf extends Aggregator<Tuple2<String, Integer>, Integer, Integer> {
    @Override
    public Integer zero() {
      return 0;
    }

    @Override
    public Integer reduce(Integer l, Tuple2<String, Integer> t) {
      return l + t._2();
    }

    @Override
    public Integer merge(Integer b1, Integer b2) {
      return b1 + b2;
    }

    @Override
    public Integer finish(Integer reduction) {
      return reduction;
    }

    @Override
    public Encoder<Integer> bufferEncoder() {
      return Encoders.INT();
    }

    @Override
    public Encoder<Integer> outputEncoder() {
      return Encoders.INT();
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationAverage() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.avg(value -> value._2() * 2.0));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3.0), new Tuple2<>("b", 6.0)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationCount() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.count(value -> value));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 2L), new Tuple2<>("b", 1L)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationSumDouble() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.sum(value -> (double) value._2()));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3.0), new Tuple2<>("b", 3.0)),
        aggregated.collectAsList());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testTypedAggregationSumLong() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> aggregated = grouped.agg(
      org.apache.spark.sql.expressions.javalang.typed.sumLong(value -> (long) value._2()));
    Assertions.assertEquals(
        Arrays.asList(new Tuple2<>("a", 3L), new Tuple2<>("b", 3L)),
        aggregated.collectAsList());
  }
}
