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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.junit.After;
import org.junit.Before;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.test.TestSparkSession;

/**
 * Common test base shared across this and Java8DatasetAggregatorSuite.
 */
public class JavaDatasetAggregatorSuiteBase implements Serializable {
  private transient TestSparkSession spark;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    spark.loadTestData();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  protected <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  protected KeyValueGroupedDataset<String, Tuple2<String, Integer>> generateGroupedDataset() {
    Encoder<Tuple2<String, Integer>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.INT());
    List<Tuple2<String, Integer>> data =
      Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 3));
    Dataset<Tuple2<String, Integer>> ds = spark.createDataset(data, encoder);

    return ds.groupByKey(
      new MapFunction<Tuple2<String, Integer>, String>() {
        @Override
        public String call(Tuple2<String, Integer> value) throws Exception {
          return value._1();
        }
      },
      Encoders.STRING());
  }
}

