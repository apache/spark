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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaApplySchemaSuite implements Serializable {
  private transient SparkSession spark;
  private transient JavaSparkContext jsc;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

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

  @Test
  public void applySchema() {
    List<Person> personList = new ArrayList<>(2);
    Person person1 = new Person();
    person1.setName("Michael");
    person1.setAge(29);
    personList.add(person1);
    Person person2 = new Person();
    person2.setName("Yin");
    person2.setAge(28);
    personList.add(person2);

    JavaRDD<Row> rowRDD = jsc.parallelize(personList).map(
        person -> RowFactory.create(person.getName(), person.getAge()));

    List<StructField> fields = new ArrayList<>(2);
    fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
    StructType schema = DataTypes.createStructType(fields);

    Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
    df.createOrReplaceTempView("people");
    List<Row> actual = spark.sql("SELECT * FROM people").collectAsList();

    List<Row> expected = new ArrayList<>(2);
    expected.add(RowFactory.create("Michael", 29));
    expected.add(RowFactory.create("Yin", 28));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void dataFrameRDDOperations() {
    List<Person> personList = new ArrayList<>(2);
    Person person1 = new Person();
    person1.setName("Michael");
    person1.setAge(29);
    personList.add(person1);
    Person person2 = new Person();
    person2.setName("Yin");
    person2.setAge(28);
    personList.add(person2);

    JavaRDD<Row> rowRDD = jsc.parallelize(personList).map(
        person -> RowFactory.create(person.getName(), person.getAge()));

    List<StructField> fields = new ArrayList<>(2);
    fields.add(DataTypes.createStructField("", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
    StructType schema = DataTypes.createStructType(fields);

    Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
    df.createOrReplaceTempView("people");
    List<String> actual = spark.sql("SELECT * FROM people").toJavaRDD()
      .map(row -> row.getString(0) + "_" + row.get(1)).collect();

    List<String> expected = new ArrayList<>(2);
    expected.add("Michael_29");
    expected.add("Yin_28");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void applySchemaToJSON() {
    Dataset<String> jsonDS = spark.createDataset(Arrays.asList(
      "{\"string\":\"this is a simple string.\", \"integer\":10, \"long\":21474836470, " +
        "\"bigInteger\":92233720368547758070, \"double\":1.7976931348623157E308, " +
        "\"boolean\":true, \"null\":null}",
      "{\"string\":\"this is another simple string.\", \"integer\":11, \"long\":21474836469, " +
        "\"bigInteger\":92233720368547758069, \"double\":1.7976931348623157E305, " +
        "\"boolean\":false, \"null\":null}"), Encoders.STRING());
    List<StructField> fields = new ArrayList<>(7);
    fields.add(DataTypes.createStructField("bigInteger", DataTypes.createDecimalType(20, 0),
      true));
    fields.add(DataTypes.createStructField("boolean", DataTypes.BooleanType, true));
    fields.add(DataTypes.createStructField("double", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("integer", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("long", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("null", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("string", DataTypes.StringType, true));
    StructType expectedSchema = DataTypes.createStructType(fields);
    List<Row> expectedResult = new ArrayList<>(2);
    expectedResult.add(
      RowFactory.create(
        new BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string."));
    expectedResult.add(
      RowFactory.create(
        new BigDecimal("92233720368547758069"),
        false,
        1.7976931348623157E305,
        11,
        21474836469L,
        null,
        "this is another simple string."));

    Dataset<Row> df1 = spark.read().json(jsonDS);
    StructType actualSchema1 = df1.schema();
    Assert.assertEquals(expectedSchema, actualSchema1);
    df1.createOrReplaceTempView("jsonTable1");
    List<Row> actual1 = spark.sql("select * from jsonTable1").collectAsList();
    Assert.assertEquals(expectedResult, actual1);

    Dataset<Row> df2 = spark.read().schema(expectedSchema).json(jsonDS);
    StructType actualSchema2 = df2.schema();
    Assert.assertEquals(expectedSchema, actualSchema2);
    df2.createOrReplaceTempView("jsonTable2");
    List<Row> actual2 = spark.sql("select * from jsonTable2").collectAsList();
    Assert.assertEquals(expectedResult, actual2);
  }
}
