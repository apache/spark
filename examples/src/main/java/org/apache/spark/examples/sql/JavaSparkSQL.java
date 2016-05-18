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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSQL {
  public static class Person implements Serializable {
    private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaSparkSQL")
      .getOrCreate();

    System.out.println("=== Data source: RDD ===");
    // Load a text file and convert each line to a Java Bean.
    String file = "examples/src/main/resources/people.txt";
    JavaRDD<Person> people = spark.read().text(file).javaRDD().map(
      new Function<String, Person>() {
        @Override
        public Person call(String line) {
          String[] parts = line.split(",");

          Person person = new Person();
          person.setName(parts[0]);
          person.setAge(Integer.parseInt(parts[1].trim()));

          return person;
        }
      });

    // Apply a schema to an RDD of Java Beans and create a temporary view
    Dataset<Row> schemaPeople = spark.createDataFrame(people, Person.class);
    schemaPeople.createOrReplaceTempView("people");

    // SQL can be run over RDDs which backs a temporary view.
    Dataset<Row> teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) {
        return "Name: " + row.getString(0);
      }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    System.out.println("=== Data source: Parquet File ===");
    // DataFrames can be saved as parquet files, maintaining the schema information.
    schemaPeople.write().parquet("people.parquet");

    // Read in the parquet file created above.
    // Parquet files are self-describing so the schema is preserved.
    // The result of loading a parquet file is also a DataFrame.
    Dataset<Row> parquetFile = spark.read().parquet("people.parquet");

    // A temporary view can be created by using Parquet files and then used in SQL statements.
    parquetFile.createOrReplaceTempView("parquetFile");
    Dataset<Row> teenagers2 =
      spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
    teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) {
          return "Name: " + row.getString(0);
      }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    System.out.println("=== Data source: JSON Dataset ===");
    // A JSON dataset is pointed by path.
    // The path can be either a single text file or a directory storing text files.
    String path = "examples/src/main/resources/people.json";
    // Create a DataFrame from the file(s) pointed by path
    Dataset<Row> peopleFromJsonFile = spark.read().json(path);

    // Because the schema of a JSON dataset is automatically inferred, to write queries,
    // it is better to take a look at what is the schema.
    peopleFromJsonFile.printSchema();
    // The schema of people is ...
    // root
    //  |-- age: IntegerType
    //  |-- name: StringType

    // Creates a temporary view using the DataFrame
    peopleFromJsonFile.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by `spark`
    Dataset<Row> teenagers3 = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

    // The results of SQL queries are DataFrame and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagerNames = teenagers3.toJavaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) { return "Name: " + row.getString(0); }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a RDD[String] storing one JSON object per string.
    List<String> jsonData = Arrays.asList(
          "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
    JavaRDD<String> anotherPeopleRDD = spark
      .createDataFrame(jsonData, String.class).toJSON().javaRDD();
    Dataset<Row> peopleFromJsonRDD = spark.read().json(anotherPeopleRDD);

    // Take a look at the schema of this new DataFrame.
    peopleFromJsonRDD.printSchema();
    // The schema of anotherPeople is ...
    // root
    //  |-- address: StructType
    //  |    |-- city: StringType
    //  |    |-- state: StringType
    //  |-- name: StringType

    peopleFromJsonRDD.createOrReplaceTempView("people2");

    Dataset<Row> peopleWithCity = spark.sql("SELECT name, address.city FROM people2");
    List<String> nameAndCity = peopleWithCity.toJavaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) {
        return "Name: " + row.getString(0) + ", City: " + row.getString(1);
      }
    }).collect();
    for (String name: nameAndCity) {
      System.out.println(name);
    }

    spark.stop();
  }
}
