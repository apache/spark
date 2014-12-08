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

package org.apache.spark.ml;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

/**
 * Test {@link LabeledPoint} in Java
 */
public class JavaLabeledPointSuite {

  private transient JavaSparkContext jsc;
  private transient JavaSQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaLabeledPointSuite");
    jsql = new JavaSQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void labeledPointDefaultWeight() {
    double label = 1.0;
    Vector features = Vectors.dense(1.0, 2.0, 3.0);
    LabeledPoint lp1 = new LabeledPoint(label, features);
    LabeledPoint lp2 = new LabeledPoint(label, features, 1.0);
    assert(lp1.equals(lp2));
  }

  @Test
  public void labeledPointSchemaRDD() {
    List<LabeledPoint> arr = Lists.newArrayList(
        new LabeledPoint(0.0, Vectors.dense(1.0, 2.0, 3.0)),
        new LabeledPoint(1.0, Vectors.dense(1.1, 2.1, 3.1)),
        new LabeledPoint(0.0, Vectors.dense(1.2, 2.2, 3.2)),
        new LabeledPoint(1.0, Vectors.dense(1.3, 2.3, 3.3)));
    JavaRDD<LabeledPoint> rdd = jsc.parallelize(arr);
    JavaSchemaRDD schemaRDD = jsql.applySchema(rdd, LabeledPoint.class);
    schemaRDD.registerTempTable("points");
    List<Row> points = jsql.sql("SELECT label, features FROM points").collect();
    assert (points.size() == arr.size());
  }
}
