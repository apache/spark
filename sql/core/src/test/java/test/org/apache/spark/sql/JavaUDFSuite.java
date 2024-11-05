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
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaUDFSuite implements Serializable {
  private transient SparkSession spark;

  @BeforeEach
  public void setUp() {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void udf1Test() {
    spark.udf().register("stringLengthTest", (String str) -> str.length(), DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test')").head();
    Assertions.assertEquals(4, result.getInt(0));
  }

  @Test
  public void udf2Test() {
    spark.udf().register("stringLengthTest",
        (String str1, String str2) -> str1.length() + str2.length(), DataTypes.IntegerType);

    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assertions.assertEquals(9, result.getInt(0));
  }

  public static class StringLengthTest implements UDF2<String, String, Integer> {
    @Override
    public Integer call(String str1, String str2) {
      return str1.length() + str2.length();
    }
  }

  @Test
  public void udf3Test() {
    spark.udf().registerJava("stringLengthTest", StringLengthTest.class.getName(),
        DataTypes.IntegerType);
    Row result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assertions.assertEquals(9, result.getInt(0));

    // returnType is not provided
    spark.udf().registerJava("stringLengthTest2", StringLengthTest.class.getName(), null);
    result = spark.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assertions.assertEquals(9, result.getInt(0));
  }

  @Test
  public void udf4Test() {
    spark.udf().register("inc", (Long i) -> i + 1, DataTypes.LongType);

    spark.range(10).toDF("x").createOrReplaceTempView("tmp");
    // This tests when Java UDFs are required to be the semantically same (See SPARK-9435).
    List<Row> results = spark.sql("SELECT inc(x) FROM tmp GROUP BY inc(x)").collectAsList();
    Assertions.assertEquals(10, results.size());
    long sum = 0;
    for (Row result : results) {
      sum += result.getLong(0);
    }
    Assertions.assertEquals(55, sum);
  }

  @Test
  public void udf5Test() {
    spark.udf().register("inc", (Long i) -> i + 1, DataTypes.LongType);
    Assertions.assertThrows(AnalysisException.class,
      () -> spark.sql("SELECT inc(1, 5)").collectAsList());
  }

  @Test
  public void udf6Test() {
    spark.udf().register("returnOne", () -> 1, DataTypes.IntegerType);
    Row result = spark.sql("SELECT returnOne()").head();
    Assertions.assertEquals(1, result.getInt(0));
  }

  @Test
  public void udf7Test() {
    String originConf = spark.conf().get(SQLConf.DATETIME_JAVA8API_ENABLED().key());
    try {
      spark.conf().set(SQLConf.DATETIME_JAVA8API_ENABLED().key(), "true");
      spark.udf().register(
          "plusDay",
          (java.time.LocalDate ld) -> ld.plusDays(1), DataTypes.DateType);
      Row result = spark.sql("SELECT plusDay(DATE '2019-02-26')").head();
      Assertions.assertEquals(LocalDate.parse("2019-02-27"), result.get(0));
    } finally {
      spark.conf().set(SQLConf.DATETIME_JAVA8API_ENABLED().key(), originConf);
    }
  }

  @Test
  public void sourceTest() {
    spark.udf().register("stringLengthTest", (String str) -> str.length(), DataTypes.IntegerType);
    ExpressionInfo info = spark.sessionState().catalog().lookupFunctionInfo(
            FunctionIdentifier.apply("stringLengthTest"));
    Assertions.assertEquals("java_udf", info.getSource());
  }
}
