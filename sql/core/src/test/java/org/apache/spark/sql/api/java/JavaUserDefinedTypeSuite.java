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
import org.apache.spark.sql.MyDenseVector;
import org.apache.spark.sql.MyLabeledPoint;

public class JavaUserDefinedTypeSuite implements Serializable {
  private transient JavaSparkContext javaCtx;
  private transient JavaSQLContext javaSqlCtx;

  @Before
  public void setUp() {
    javaCtx = new JavaSparkContext("local", "JavaUserDefinedTypeSuite");
    javaSqlCtx = new JavaSQLContext(javaCtx);
  }

  @After
  public void tearDown() {
    javaCtx.stop();
    javaCtx = null;
    javaSqlCtx = null;
  }

  @Test
  public void useScalaUDT() {
    List<MyLabeledPoint> points = Arrays.asList(
        new MyLabeledPoint(1.0, new MyDenseVector(new double[]{0.1, 1.0})),
        new MyLabeledPoint(0.0, new MyDenseVector(new double[]{0.2, 2.0})));
    JavaRDD<MyLabeledPoint> pointsRDD = javaCtx.parallelize(points);

    JavaSchemaRDD schemaRDD = javaSqlCtx.applySchema(pointsRDD, MyLabeledPoint.class);
    schemaRDD.registerTempTable("points");

    List<Row> actualLabelRows = javaSqlCtx.sql("SELECT label FROM points").collect();
    List<Double> actualLabels = new LinkedList<Double>();
    for (Row r : actualLabelRows) {
      actualLabels.add(r.getDouble(0));
    }
    for (MyLabeledPoint lp : points) {
      Assert.assertTrue(actualLabels.contains(lp.label()));
    }

    List<Row> actualFeatureRows = javaSqlCtx.sql("SELECT features FROM points").collect();
    List<MyDenseVector> actualFeatures = new LinkedList<MyDenseVector>();
    for (Row r : actualFeatureRows) {
      actualFeatures.add((MyDenseVector)r.get(0));
    }
    for (MyLabeledPoint lp : points) {
      Assert.assertTrue(actualFeatures.contains(lp.features()));
    }

    List<Row> actual = javaSqlCtx.sql("SELECT label, features FROM points").collect();
    List<MyLabeledPoint> actualPoints =
        new LinkedList<MyLabeledPoint>();
    for (Row r : actual) {
      actualPoints.add(new MyLabeledPoint(r.getDouble(0), (MyDenseVector)r.get(1)));
    }
    for (MyLabeledPoint lp : points) {
      Assert.assertTrue(actualPoints.contains(lp));
    }
  }
}
