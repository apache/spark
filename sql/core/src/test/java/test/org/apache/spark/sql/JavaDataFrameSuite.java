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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.test.TestSQLContext$;
import static org.apache.spark.sql.functions.*;


public class JavaDataFrameSuite {
  private transient SQLContext context;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    TestData$.MODULE$.testData();
    context = TestSQLContext$.MODULE$;
  }

  @After
  public void tearDown() {
    context = null;
  }

  @Test
  public void testExecution() {
    DataFrame df = context.table("testData").filter("key = 1");
    Assert.assertEquals(df.select("key").collect()[0].get(0), 1);
  }

  /**
   * See SPARK-5904. Abstract vararg methods defined in Scala do not work in Java.
   */
  @Test
  public void testVarargMethods() {
    DataFrame df = context.table("testData");

    df.toDF("key1", "value1");

    df.select("key", "value");
    df.select(col("key"), col("value"));
    df.selectExpr("key", "value + 1");

    df.sort("key", "value");
    df.sort(col("key"), col("value"));
    df.orderBy("key", "value");
    df.orderBy(col("key"), col("value"));

    df.groupBy("key", "value").agg(col("key"), col("value"), sum("value"));
    df.groupBy(col("key"), col("value")).agg(col("key"), col("value"), sum("value"));
    df.agg(first("key"), sum("value"));

    df.groupBy().avg("key");
    df.groupBy().mean("key");
    df.groupBy().max("key");
    df.groupBy().min("key");
    df.groupBy().sum("key");

    // Varargs in column expressions
    df.groupBy().agg(countDistinct("key", "value"));
    df.groupBy().agg(countDistinct(col("key"), col("value")));
    df.select(coalesce(col("key")));
  }

  @Ignore
  public void testShow() {
    // This test case is intended ignored, but to make sure it compiles correctly
    DataFrame df = context.table("testData");
    df.show();
    df.show(1000);
  }
}
