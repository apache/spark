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

// $example on:typed_custom_aggregation$
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;
// $example off:typed_custom_aggregation$

public class JavaUserDefinedTypedAggregation {

  // $example on:typed_custom_aggregation$
  public static class Employee implements Serializable {
    private String name;
    private long salary;

    // Constructors, getters, setters...
    // $example off:typed_custom_aggregation$
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getSalary() {
      return salary;
    }

    public void setSalary(long salary) {
      this.salary = salary;
    }
    // $example on:typed_custom_aggregation$
  }

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

  public static class MyAverage extends Aggregator<Employee, Average, Double> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    @Override
    public Average zero() {
      return new Average(0L, 0L);
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    @Override
    public Average reduce(Average buffer, Employee employee) {
      long newSum = buffer.getSum() + employee.getSalary();
      long newCount = buffer.getCount() + 1;
      buffer.setSum(newSum);
      buffer.setCount(newCount);
      return buffer;
    }
    // Merge two intermediate values
    @Override
    public Average merge(Average b1, Average b2) {
      long mergedSum = b1.getSum() + b2.getSum();
      long mergedCount = b1.getCount() + b2.getCount();
      b1.setSum(mergedSum);
      b1.setCount(mergedCount);
      return b1;
    }
    // Transform the output of the reduction
    @Override
    public Double finish(Average reduction) {
      return ((double) reduction.getSum()) / reduction.getCount();
    }
    // Specifies the Encoder for the intermediate value type
    @Override
    public Encoder<Average> bufferEncoder() {
      return Encoders.bean(Average.class);
    }
    // Specifies the Encoder for the final output value type
    @Override
    public Encoder<Double> outputEncoder() {
      return Encoders.DOUBLE();
    }
  }
  // $example off:typed_custom_aggregation$

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL user-defined Datasets aggregation example")
      .getOrCreate();

    // $example on:typed_custom_aggregation$
    Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
    String path = "examples/src/main/resources/employees.json";
    Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
    ds.show();
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    MyAverage myAverage = new MyAverage();
    // Convert the function to a `TypedColumn` and give it a name
    TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
    Dataset<Double> result = ds.select(averageSalary);
    result.show();
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:typed_custom_aggregation$
    spark.stop();
  }

}
