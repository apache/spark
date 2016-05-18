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

package test.org.apache.spark.sql.sources;

import java.util.Arrays;

import scala.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.javalang.typed;

/**
 * Suite for testing the aggregate functionality of Datasets in Java.
 */
public class JavaDatasetAggregatorSuite extends JavaDatasetAggregatorSuiteBase {
  @Test
  public void testTypedAggregationAnonClass() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();

    Dataset<Tuple2<String, Integer>> agged = grouped.agg(new IntSumOf().toColumn());
    Assert.assertEquals(Arrays.asList(tuple2("a", 3), tuple2("b", 3)), agged.collectAsList());

    Dataset<Tuple2<String, Integer>> agged2 = grouped.agg(new IntSumOf().toColumn())
      .as(Encoders.tuple(Encoders.STRING(), Encoders.INT()));
    Assert.assertEquals(
      Arrays.asList(
        new Tuple2<>("a", 3),
        new Tuple2<>("b", 3)),
      agged2.collectAsList());
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

  @Test
  public void testTypedAggregationAverage() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> agged = grouped.agg(typed.avg(
      new MapFunction<Tuple2<String, Integer>, Double>() {
        public Double call(Tuple2<String, Integer> value) throws Exception {
          return (double)(value._2() * 2);
        }
      }));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3.0), tuple2("b", 6.0)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationCount() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> agged = grouped.agg(typed.count(
      new MapFunction<Tuple2<String, Integer>, Object>() {
        public Object call(Tuple2<String, Integer> value) throws Exception {
          return value;
        }
      }));
    Assert.assertEquals(Arrays.asList(tuple2("a", 2), tuple2("b", 1)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationSumDouble() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Double>> agged = grouped.agg(typed.sum(
      new MapFunction<Tuple2<String, Integer>, Double>() {
        public Double call(Tuple2<String, Integer> value) throws Exception {
          return (double)value._2();
        }
      }));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3.0), tuple2("b", 3.0)), agged.collectAsList());
  }

  @Test
  public void testTypedAggregationSumLong() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();
    Dataset<Tuple2<String, Long>> agged = grouped.agg(typed.sumLong(
      new MapFunction<Tuple2<String, Integer>, Long>() {
        public Long call(Tuple2<String, Integer> value) throws Exception {
          return (long)value._2();
        }
      }));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3), tuple2("b", 3)), agged.collectAsList());
  }
}
