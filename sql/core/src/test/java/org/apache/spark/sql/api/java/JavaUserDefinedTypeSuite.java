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
import java.util.*;

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
    //JavaSQLContext$.MODULE$.registerUDT(new MyDenseVectorUDT());
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

  class MyLabeledPoint implements Serializable {
    double label;
    MyDenseVector features;
    public MyLabeledPoint(double label, MyDenseVector features) {
      this.label = label;
      this.features = features;
    }
  }

  class MyDenseVectorUDT extends UserDefinedType<MyDenseVector> implements Serializable {

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
    JavaRDD<org.apache.spark.sql.MyLabeledPoint> pointsRDD = javaCtx.parallelize(points);

    JavaSchemaRDD schemaRDD =
        javaSqlCtx.applySchema(pointsRDD, org.apache.spark.sql.MyLabeledPoint.class);

    schemaRDD.registerTempTable("points");
    List<Row> actual = javaSqlCtx.sql("SELECT * FROM points").collect();
    List<org.apache.spark.sql.MyLabeledPoint> actualPoints =
        new LinkedList<org.apache.spark.sql.MyLabeledPoint>();
    for (Row r : actual) {
      // Note: JavaSQLContext.getSchema switches the ordering of the Row elements
      //       in the MyLabeledPoint case class.
      actualPoints.add(new org.apache.spark.sql.MyLabeledPoint(
          r.getDouble(1), (org.apache.spark.sql.MyDenseVector)r.get(0)));
    }
    for (org.apache.spark.sql.MyLabeledPoint lp : points) {
      Assert.assertTrue(actualPoints.contains(lp));
    }
    /*
    // THIS FAILS BECAUSE JavaSQLContext.getSchema switches the ordering of the Row elements
    //  in the MyLabeledPoint case class.
    List<Row> expected = new LinkedList<Row>();
    expected.add(Row.create(new org.apache.spark.sql.MyLabeledPoint(1.0,
        new org.apache.spark.sql.MyDenseVector(new double[]{0.1, 1.0}))));
    expected.add(Row.create(new org.apache.spark.sql.MyLabeledPoint(0.0,
        new org.apache.spark.sql.MyDenseVector(new double[]{0.2, 2.0}))));
    System.out.println("Expected:");
    for (Row r : expected) {
      System.out.println("r: " + r.toString());
      for (int i = 0; i < r.length(); ++i) {
        System.out.println("  r[i]: " + r.get(i).toString());
      }
    }

    System.out.println("Actual:");
    for (Row r : actual) {
      System.out.println("r: " + r.toString());
      for (int i = 0; i < r.length(); ++i) {
        System.out.println("  r[i]: " + r.get(i).toString());
      }
      Assert.assertTrue(expected.contains(r));
    }
    */
  }
}
