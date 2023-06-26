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

import org.apache.spark.sql.*;
import org.apache.spark.sql.test.TestSparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaSparkSessionSuite {
  private SparkSession spark;

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void config() {
    // SPARK-40163: SparkSession.config(Map)
    Map<String, Object> map = new HashMap<String, Object>() {{
      put("string", "");
      put("boolean", true);
      put("double", 0.0);
      put("long", 0L);
    }};

    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .config(map)
      .getOrCreate();

    for (Map.Entry<String, Object> e : map.entrySet()) {
      Assert.assertEquals(spark.conf().get(e.getKey()), e.getValue().toString());
    }
  }

  @Test
  public void testPositionalParameters() {
    spark = new TestSparkSession();

    int[] emptyArgs = {};
    List<Row> collected1 = spark.sql("select 'abc'", emptyArgs).collectAsList();
    Assert.assertEquals("abc", collected1.get(0).getString(0));

    Object[] singleArg = new String[] { "abc" };
    List<Row> collected2 = spark.sql("select ?", singleArg).collectAsList();
    Assert.assertEquals("abc", collected2.get(0).getString(0));

    int[] args = new int[] { 1, 2, 3 };
    List<Row> collected3 = spark.sql("select ?, ?, ?", args).collectAsList();
    Row r0 = collected3.get(0);
    Assert.assertEquals(1, r0.getInt(0));
    Assert.assertEquals(2, r0.getInt(1));
    Assert.assertEquals(3, r0.getInt(2));

    Object[] mixedArgs = new Object[] { 1, "abc" };
    List<Row> collected4 = spark.sql("select ?, ?", mixedArgs).collectAsList();
    Row r1 = collected4.get(0);
    Assert.assertEquals(1, r1.getInt(0));
    Assert.assertEquals("abc", r1.getString(1));
  }
}
