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

package org.apache.spark.sql.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.hive.aggregate.MyDoubleSum;

public class JavaDataFrameSuite {
  private transient JavaSparkContext sc;
  private transient HiveContext hc;

  DataFrame df;

  private static void checkAnswer(DataFrame actual, List<Row> expected) {
    String errorMessage = QueryTest$.MODULE$.checkAnswer(actual, expected);
    if (errorMessage != null) {
      Assert.fail(errorMessage);
    }
  }

  @Before
  public void setUp() throws IOException {
    hc = TestHive$.MODULE$;
    sc = new JavaSparkContext(hc.sparkContext());

    List<String> jsonObjects = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      jsonObjects.add("{\"key\":" + i + ", \"value\":\"str" + i + "\"}");
    }
    df = hc.read().json(sc.parallelize(jsonObjects));
    df.registerTempTable("window_table");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up tables.
    if (hc != null) {
      hc.sql("DROP TABLE IF EXISTS window_table");
    }
  }

  @Test
  public void saveTableAndQueryIt() {
    checkAnswer(
      df.select(avg("key").over(
        Window.partitionBy("value").orderBy("key").rowsBetween(-1, 1))),
      hc.sql("SELECT avg(key) " +
        "OVER (PARTITION BY value " +
        "      ORDER BY key " +
        "      ROWS BETWEEN 1 preceding and 1 following) " +
        "FROM window_table").collectAsList());
  }

  @Test
  public void testUDAF() {
    DataFrame df = hc.range(0, 100).unionAll(hc.range(0, 100)).select(col("id").as("value"));
    UserDefinedAggregateFunction udaf = new MyDoubleSum();
    UserDefinedAggregateFunction registeredUDAF = hc.udf().register("mydoublesum", udaf);
    // Create Columns for the UDAF. For now, callUDF does not take an argument to specific if
    // we want to use distinct aggregation.
    DataFrame aggregatedDF =
      df.groupBy()
        .agg(
          udaf.distinct(col("value")),
          udaf.apply(col("value")),
          registeredUDAF.apply(col("value")),
          callUDF("mydoublesum", col("value")));

    List<Row> expectedResult = new ArrayList<>();
    expectedResult.add(RowFactory.create(4950.0, 9900.0, 9900.0, 9900.0));
    checkAnswer(
      aggregatedDF,
      expectedResult);
  }
}
