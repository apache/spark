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

// $example on:untyped_custom_aggregation$
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;
// $example off:untyped_custom_aggregation$

public class JavaUserDefinedUntypedAggregation {

  // $example on:untyped_custom_aggregation$
  public static class Average implements Serializable  {
    private long sum;
    private long count;

    // Constructors, getters, setters...
    // $example off:typed_custom_aggregation$
    public Average() {
    }

    public Average(long sum, long count) {
      this.sum = sum;
      this.count = count;
    }

    public long getSum() {
      return sum;
    }

    public void setSum(long sum) {
      this.sum = sum;
    }

    public long getCount() {
      return count;
    }

    public void setCount(long count) {
      this.count = count;
    }
    // $example on:typed_custom_aggregation$
  }

  public static class MyAverage extends Aggregator<Long, Average, Double> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    public Average zero() {
      return new Average(0L, 0L);
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    public Average reduce(Average buffer, Long data) {
      long newSum = buffer.getSum() + data;
      long newCount = buffer.getCount() + 1;
      buffer.setSum(newSum);
      buffer.setCount(newCount);
      return buffer;
    }
    // Merge two intermediate values
    public Average merge(Average b1, Average b2) {
      long mergedSum = b1.getSum() + b2.getSum();
      long mergedCount = b1.getCount() + b2.getCount();
      b1.setSum(mergedSum);
      b1.setCount(mergedCount);
      return b1;
    }
    // Transform the output of the reduction
    public Double finish(Average reduction) {
      return ((double) reduction.getSum()) / reduction.getCount();
    }
    // Specifies the Encoder for the intermediate value type
    public Encoder<Average> bufferEncoder() {
      return Encoders.bean(Average.class);
    }
    // Specifies the Encoder for the final output value type
    public Encoder<Double> outputEncoder() {
      return Encoders.DOUBLE();
    }
  }
  // $example off:untyped_custom_aggregation$

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate();

    // $example on:untyped_custom_aggregation$
    // Register the function to access it
    spark.udf().register("myAverage", functions.udaf(new MyAverage(), Encoders.LONG()));

    Dataset<Row> df = spark.read().json("examples/src/main/resources/employees.json");
    df.createOrReplaceTempView("employees");
    df.show();
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
    result.show();
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:untyped_custom_aggregation$

    spark.stop();
  }
}
