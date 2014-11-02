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
import org.apache.spark.api.java.function.Function;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaApplySchemaSuite implements Serializable {
  private transient JavaSparkContext javaCtx;
  private transient JavaSQLContext javaSqlCtx;

  @Before
  public void setUp() {
    javaCtx = new JavaSparkContext("local", "JavaApplySchemaSuite");
    javaSqlCtx = new JavaSQLContext(javaCtx);
  }

  @After
  public void tearDown() {
    javaCtx.stop();
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
          return Row.create(person.getName(), person.getAge());
        }
      });

    List<StructField> fields = new ArrayList<StructField>(2);
    fields.add(DataType.createStructField("name", DataType.StringType, false));
    fields.add(DataType.createStructField("age", DataType.IntegerType, false));
    StructType schema = DataType.createStructType(fields);

    JavaSchemaRDD schemaRDD = javaSqlCtx.applySchema(rowRDD, schema);
    schemaRDD.registerTempTable("people");
    List<Row> actual = javaSqlCtx.sql("SELECT * FROM people").collect();

    List<Row> expected = new ArrayList<Row>(2);
    expected.add(Row.create("Michael", 29));
    expected.add(Row.create("Yin", 28));

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
    fields.add(DataType.createStructField("bigInteger", new DecimalType(), true));
    fields.add(DataType.createStructField("boolean", DataType.BooleanType, true));
    fields.add(DataType.createStructField("double", DataType.DoubleType, true));
    fields.add(DataType.createStructField("integer", DataType.IntegerType, true));
    fields.add(DataType.createStructField("long", DataType.LongType, true));
    fields.add(DataType.createStructField("null", DataType.StringType, true));
    fields.add(DataType.createStructField("string", DataType.StringType, true));
    StructType expectedSchema = DataType.createStructType(fields);
    List<Row> expectedResult = new ArrayList<Row>(2);
    expectedResult.add(
      Row.create(
        new BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string."));
    expectedResult.add(
      Row.create(
        new BigDecimal("92233720368547758069"),
        false,
        1.7976931348623157E305,
        11,
        21474836469L,
        null,
        "this is another simple string."));

    JavaSchemaRDD schemaRDD1 = javaSqlCtx.jsonRDD(jsonRDD);
    StructType actualSchema1 = schemaRDD1.schema();
    Assert.assertEquals(expectedSchema, actualSchema1);
    schemaRDD1.registerTempTable("jsonTable1");
    List<Row> actual1 = javaSqlCtx.sql("select * from jsonTable1").collect();
    Assert.assertEquals(expectedResult, actual1);

    JavaSchemaRDD schemaRDD2 = javaSqlCtx.jsonRDD(jsonRDD, expectedSchema);
    StructType actualSchema2 = schemaRDD2.schema();
    Assert.assertEquals(expectedSchema, actualSchema2);
    schemaRDD2.registerTempTable("jsonTable2");
    List<Row> actual2 = javaSqlCtx.sql("select * from jsonTable2").collect();
    Assert.assertEquals(expectedResult, actual2);
  }
}
