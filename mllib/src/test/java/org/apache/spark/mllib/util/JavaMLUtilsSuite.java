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

package org.apache.spark.mllib.util;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class JavaMLUtilsSuite extends SharedSparkSession {

  @Test
  public void testConvertVectorColumnsToAndFromML() {
    Vector x = Vectors.dense(2.0);
    Dataset<Row> dataset = spark.createDataFrame(
      Collections.singletonList(new LabeledPoint(1.0, x)), LabeledPoint.class
    ).select("label", "features");
    Dataset<Row> newDataset1 = MLUtils.convertVectorColumnsToML(dataset);
    Row new1 = newDataset1.first();
    Assert.assertEquals(RowFactory.create(1.0, x.asML()), new1);
    Row new2 = MLUtils.convertVectorColumnsToML(dataset, "features").first();
    Assert.assertEquals(new1, new2);
    Row old1 = MLUtils.convertVectorColumnsFromML(newDataset1).first();
    Assert.assertEquals(RowFactory.create(1.0, x), old1);
  }
}
