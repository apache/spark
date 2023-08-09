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
package org.apache.spark.examples.sql;

// $example on:udf_scalar$
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;
import org.apache.spark.sql.types.DataTypes;
// $example off:udf_scalar$

public class JavaUserDefinedScalar {

  public static void main(String[] args) {

    // $example on:udf_scalar$
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL UDF scalar example")
      .getOrCreate();

    // Define and register a zero-argument non-deterministic UDF
    // UDF is deterministic by default, i.e. produces the same result for the same input.
    UserDefinedFunction random = udf(
      () -> Math.random(), DataTypes.DoubleType
    );
    random.asNondeterministic();
    spark.udf().register("random", random);
    spark.sql("SELECT random()").show();
    // +-------+
    // |UDF()  |
    // +-------+
    // |xxxxxxx|
    // +-------+

    // Define and register a one-argument UDF
    spark.udf().register("plusOne",
      (UDF1<Integer, Integer>) x -> x + 1, DataTypes.IntegerType);
    spark.sql("SELECT plusOne(5)").show();
    // +----------+
    // |plusOne(5)|
    // +----------+
    // |         6|
    // +----------+

    // Define and register a two-argument UDF
    UserDefinedFunction strLen = udf(
      (String s, Integer x) -> s.length() + x, DataTypes.IntegerType
    );
    spark.udf().register("strLen", strLen);
    spark.sql("SELECT strLen('test', 1)").show();
    // +------------+
    // |UDF(test, 1)|
    // +------------+
    // |           5|
    // +------------+

    // UDF in a WHERE clause
    spark.udf().register("oneArgFilter",
      (UDF1<Long, Boolean>) x -> x > 5, DataTypes.BooleanType);
    spark.range(1, 10).createOrReplaceTempView("test");
    spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();
    // +---+
    // | id|
    // +---+
    // |  6|
    // |  7|
    // |  8|
    // |  9|
    // +---+

    // $example off:udf_scalar$
    spark.stop();
  }
}
