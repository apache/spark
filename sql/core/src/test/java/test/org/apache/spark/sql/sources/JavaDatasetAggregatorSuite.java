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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.java.typed;
import org.apache.spark.sql.test.TestSQLContext;

/**
 * Suite for testing the aggregate functionality of Datasets in Java.
 */
public class JavaDatasetAggregatorSuite implements Serializable {
  private transient JavaSparkContext jsc;
  private transient TestSQLContext context;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    SparkContext sc = new SparkContext("local[*]", "testing");
    jsc = new JavaSparkContext(sc);
    context = new TestSQLContext(sc);
    context.loadTestData();
  }

  @After
  public void tearDown() {
    context.sparkContext().stop();
    context = null;
    jsc = null;
  }

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  private KeyValueGroupedDataset<String, Tuple2<String, Integer>> generateGroupedDataset() {
    Encoder<Tuple2<String, Integer>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.INT());
    List<Tuple2<String, Integer>> data =
      Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 3));
    Dataset<Tuple2<String, Integer>> ds = context.createDataset(data, encoder);

    return ds.groupByKey(
      new MapFunction<Tuple2<String, Integer>, String>() {
        @Override
        public String call(Tuple2<String, Integer> value) throws Exception {
          return value._1();
        }
      },
      Encoders.STRING());
  }

  @Test
  public void testTypedAggregationAnonClass() {
    KeyValueGroupedDataset<String, Tuple2<String, Integer>> grouped = generateGroupedDataset();

    Dataset<Tuple2<String, Integer>> agged =
      grouped.agg(new IntSumOf().toColumn(Encoders.INT(), Encoders.INT()));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3), tuple2("b", 3)), agged.collectAsList());

    Dataset<Tuple2<String, Integer>> agged2 = grouped.agg(
      new IntSumOf().toColumn(Encoders.INT(), Encoders.INT()))
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
