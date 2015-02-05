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

package org.apache.spark.sql.api.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.test.TestSQLContext$;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaApplySchemaSuite implements Serializable {
  private transient JavaSparkContext javaCtx;
  private transient SQLContext javaSqlCtx;

  @Before
  public void setUp() {
    javaSqlCtx = TestSQLContext$.MODULE$;
    javaCtx = new JavaSparkContext(javaSqlCtx.sparkContext());
  }

  @After
  public void tearDown() {
    javaCtx = null;
    javaSqlCtx = null;
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
    List<Person> personList = new ArrayList<Person>(2);
    Person person1 = new Person();
    person1.setName("Michael");
    person1.setAge(29);
    personList.add(person1);
    Person person2 = new Person();
    person2.setName("Yin");
    person2.setAge(28);
    personList.add(person2);

    JavaRDD<Row> rowRDD = javaCtx.parallelize(personList).map(
      new Function<Person, Row>() {
        public Row call(Person person) throws Exception {
          return RowFactory.create(person.getName(), person.getAge());
        }
      });

    List<StructField> fields = new ArrayList<StructField>(2);
    fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
    StructType schema = DataTypes.createStructType(fields);

    DataFrame df = javaSqlCtx.applySchema(rowRDD, schema);
    df.registerTempTable("people");
    Row[] actual = javaSqlCtx.sql("SELECT * FROM people").collect();

    List<Row> expected = new ArrayList<Row>(2);
    expected.add(RowFactory.create("Michael", 29));
    expected.add(RowFactory.create("Yin", 28));

    Assert.assertEquals(expected, Arrays.asList(actual));
  }



  @Test
  public void dataFrameRDDOperations() {
    List<Person> personList = new ArrayList<Person>(2);
    Person person1 = new Person();
    person1.setName("Michael");
    person1.setAge(29);
    personList.add(person1);
    Person person2 = new Person();
    person2.setName("Yin");
    person2.setAge(28);
    personList.add(person2);

    JavaRDD<Row> rowRDD = javaCtx.parallelize(personList).map(
            new Function<Person, Row>() {
              public Row call(Person person) throws Exception {
                return RowFactory.create(person.getName(), person.getAge());
              }
            });

    List<StructField> fields = new ArrayList<StructField>(2);
    fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, false));
    StructType schema = DataTypes.createStructType(fields);

    DataFrame df = javaSqlCtx.applySchema(rowRDD, schema);
    df.registerTempTable("people");
    List<String> actual = javaSqlCtx.sql("SELECT * FROM people").toJavaRDD().map(new Function<Row, String>() {

      public String call(Row row) {
        return row.getString(0) + "_" + row.get(1).toString();
      }
    }).collect();

    List<String> expected = new ArrayList<String>(2);
    expected.add("Michael_29");
    expected.add("Yin_28");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void applySchemaToJSON() {
    JavaRDD<String> jsonRDD = javaCtx.parallelize(Arrays.asList(
      "{\"string\":\"this is a simple string.\", \"integer\":10, \"long\":21474836470, " +
        "\"bigInteger\":92233720368547758070, \"double\":1.7976931348623157E308, " +
        "\"boolean\":true, \"null\":null}",
      "{\"string\":\"this is another simple string.\", \"integer\":11, \"long\":21474836469, " +
        "\"bigInteger\":92233720368547758069, \"double\":1.7976931348623157E305, " +
        "\"boolean\":false, \"null\":null}"));
    List<StructField> fields = new ArrayList<StructField>(7);
    fields.add(DataTypes.createStructField("bigInteger", DataTypes.createDecimalType(), true));
    fields.add(DataTypes.createStructField("boolean", DataTypes.BooleanType, true));
    fields.add(DataTypes.createStructField("double", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("integer", DataTypes.IntegerType, true));
    fields.add(DataTypes.createStructField("long", DataTypes.LongType, true));
    fields.add(DataTypes.createStructField("null", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("string", DataTypes.StringType, true));
    StructType expectedSchema = DataTypes.createStructType(fields);
    List<Row> expectedResult = new ArrayList<Row>(2);
    expectedResult.add(
      RowFactory.create(
        new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string."));
    expectedResult.add(
      RowFactory.create(
        new java.math.BigDecimal("92233720368547758069"),
        false,
        1.7976931348623157E305,
        11,
        21474836469L,
        null,
        "this is another simple string."));

    DataFrame df1 = javaSqlCtx.jsonRDD(jsonRDD);
    StructType actualSchema1 = df1.schema();
    Assert.assertEquals(expectedSchema, actualSchema1);
    df1.registerTempTable("jsonTable1");
    List<Row> actual1 = javaSqlCtx.sql("select * from jsonTable1").collectAsList();
    Assert.assertEquals(expectedResult, actual1);

    DataFrame df2 = javaSqlCtx.jsonRDD(jsonRDD, expectedSchema);
    StructType actualSchema2 = df2.schema();
    Assert.assertEquals(expectedSchema, actualSchema2);
    df2.registerTempTable("jsonTable2");
    List<Row> actual2 = javaSqlCtx.sql("select * from jsonTable2").collectAsList();
    Assert.assertEquals(expectedResult, actual2);
  }
}
