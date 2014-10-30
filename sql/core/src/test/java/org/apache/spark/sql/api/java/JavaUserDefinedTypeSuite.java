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

import org.apache.spark.sql.types.util.DataTypeConversions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class JavaUserDefinedTypeSuite implements Serializable {
  private transient JavaSparkContext javaCtx;
  private transient JavaSQLContext javaSqlCtx;

  @Before
  public void setUp() {
    javaCtx = new JavaSparkContext("local", "JavaUserDefinedTypeSuite");
    javaSqlCtx = new JavaSQLContext(javaCtx);
    JavaSQLContext$.MODULE$.registerUDT(new MyDenseVectorUDT());
  }

  @After
  public void tearDown() {
    javaCtx.stop();
    javaCtx = null;
    javaSqlCtx = null;
  }

  // Note: Annotation is awkward since it requires an argument which is a Scala UserDefinedType.
  //@SQLUserDefinedType(udt = MyDenseVectorUDT.class)
  class MyDenseVector implements Serializable {

    public MyDenseVector(double[] data) {
      this.data = data;
    }

    public double[] data;

    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      MyDenseVector dv = (MyDenseVector) other;
      return Arrays.equals(this.data, dv.data);
    }
  }

  class MyLabeledPoint {
    double label;
    MyDenseVector features;
    public MyLabeledPoint(double label, MyDenseVector features) {
      this.label = label;
      this.features = features;
    }
  }

  class MyDenseVectorUDT extends UserDefinedType<MyDenseVector> {

    /**
     * Underlying storage type for this UDT
     */
    public DataType sqlType() {
      return DataType.createArrayType(DataType.DoubleType, false);
    }

    /**
     * Convert the user type to a SQL datum
     */
    public Object serialize(Object obj) {
      return ((MyDenseVector) obj).data;
    }

    /**
     * Convert a SQL datum to the user type
     */
    public MyDenseVector deserialize(Object datum) {
      return new MyDenseVector((double[]) datum);
    }

    public Class<MyDenseVector> userClass() {
      return MyDenseVector.class;
    }
  }

  @Test
  public void useScalaUDT() {
    List<org.apache.spark.sql.MyLabeledPoint> points = Arrays.asList(
        new org.apache.spark.sql.MyLabeledPoint(1.0,
            new org.apache.spark.sql.MyDenseVector(new double[]{0.1, 1.0})),
        new org.apache.spark.sql.MyLabeledPoint(0.0,
            new org.apache.spark.sql.MyDenseVector(new double[]{0.2, 2.0})));
    JavaRDD<Row> rowRDD = javaCtx.parallelize(points).map(
        new Function<org.apache.spark.sql.MyLabeledPoint, Row>() {
          public Row call(org.apache.spark.sql.MyLabeledPoint lp) throws Exception {
            return Row.create(lp.label(), lp.features());
          }
        });

    List<StructField> fields = new ArrayList<StructField>(2);
    fields.add(DataType.createStructField("label", DataType.DoubleType, false));
    // EDITING HERE: HOW TO CONVERT SCALA UDT TO JAVA UDT (without exposing Catalyst)?
    fields.add(DataType.createStructField("features", ???, false));
    StructType schema = DataType.createStructType(fields);

    JavaSchemaRDD schemaRDD =
        javaSqlCtx.applySchema(pointRDD, org.apache.spark.sql.MyLabeledPoint.class);

    schemaRDD.registerTempTable("points");
    List<Row> actual = javaSqlCtx.sql("SELECT * FROM points").collect();
// check set
    List<Row> expected = new ArrayList<Row>(2);
    expected.add(Row.create(1.0,
        new org.apache.spark.sql.MyDenseVector(new double[]{0.1, 1.0})));
    expected.add(Row.create(0.0,
        new org.apache.spark.sql.MyDenseVector(new double[]{0.2, 2.0})));

    Assert.assertEquals(expected, actual);
  }

    // test("register user type: MyDenseVector for MyLabeledPoint")
  @Test
  public void registerUDT() {
    /*
    List<MyLabeledPoint> points = Arrays.asList(
        new MyLabeledPoint(1.0, new MyDenseVector(new double[]{0.1, 1.0})),
        new MyLabeledPoint(0.0, new MyDenseVector(new double[]{0.2, 2.0})));
    JavaRDD<MyLabeledPoint> pointsRDD = javaCtx.parallelize(points).map(
        new Function<MyLabeledPoint, Row>() {
          public Row call(MyLabeledPoint lp) throws Exception {
            return Row.create(lp.label, lp.features)
          }
        }
    );
    JavaSchemaRDD schemaRDD = pointsRDD;
    */
    /*
    JavaRDD<Double> labels = pointsRDD.select('label).map { case Row(v: double) => v }
        val labelsArrays: Array[double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDenseVector] =
        pointsRDD.select('features).map { case Row(v: MyDenseVector) => v }
            val featuresArrays: Array[MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector(Array(0.2, 2.0))))
    */

/*
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

 */
  }

}
