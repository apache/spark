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

package org.apache.spark.ml.feature;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;


public class JavaVectorSlicerSuite extends SharedSparkSession {

  @Test
  public void vectorSlice() {
    Attribute[] attrs = new Attribute[]{
      NumericAttribute.defaultAttr().withName("f1"),
      NumericAttribute.defaultAttr().withName("f2"),
      NumericAttribute.defaultAttr().withName("f3")
    };
    AttributeGroup group = new AttributeGroup("userFeatures", attrs);

    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.sparse(3, new int[]{0, 1}, new double[]{-2.0, 2.3})),
      RowFactory.create(Vectors.dense(-2.0, 2.3, 0.0))
    );

    Dataset<Row> dataset =
      spark.createDataFrame(data, (new StructType()).add(group.toStructField()));

    VectorSlicer vectorSlicer = new VectorSlicer()
      .setInputCol("userFeatures").setOutputCol("features");

    vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});

    Dataset<Row> output = vectorSlicer.transform(dataset);

    for (Row r : output.select("userFeatures", "features").takeAsList(2)) {
      Vector features = r.getAs(1);
      Assert.assertEquals(2, features.size());
    }
  }
}
