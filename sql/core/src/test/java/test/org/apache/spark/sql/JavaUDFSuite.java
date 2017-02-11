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
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaUDFSuite implements Serializable {
  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf1Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest", (String str) -> str.length(), DataType.IntegerType);

    spark.udf().register("stringLengthTest", new UDF1<String, Integer>() {
      @Override
      public Integer call(String str) {
        return str.length();
      }
    }, DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test')").head();
    Assert.assertEquals(4, result.getInt(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf2Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest",
    //   (String str1, String str2) -> str1.length() + str2.length,
    //   DataType.IntegerType);

    spark.udf().register("stringLengthTest", new UDF2<String, String, Integer>() {
      @Override
      public Integer call(String str1, String str2) {
        return str1.length() + str2.length();
      }
    }, DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));
  }

  public static class StringLengthTest implements UDF2<String, String, Integer> {
    @Override
    public Integer call(String str1, String str2) throws Exception {
      return new Integer(str1.length() + str2.length());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf3Test() {
    spark.udf().registerJava("stringLengthTest", StringLengthTest.class.getName(),
        DataTypes.IntegerType);
    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));

    // returnType is not provided
    spark.udf().registerJava("stringLengthTest2", StringLengthTest.class.getName(), null);
    result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf4Test() {
    spark.udf().register("inc", new UDF1<Long, Long>() {
      @Override
      public Long call(Long i) {
        return i + 1;
      }
    }, DataTypes.LongType);

    spark.range(10).toDF("x").createOrReplaceTempView("tmp");
    // This tests when Java UDFs are required to be the semantically same (See SPARK-9435).
    List<Row> results = spark.sql("SELECT inc(x) FROM tmp GROUP BY inc(x)").collectAsList();
    Assert.assertEquals(10, results.size());
    long sum = 0;
    for (Row result : results) {
      sum += result.getLong(0);
    }
    Assert.assertEquals(55, sum);
  }
}
