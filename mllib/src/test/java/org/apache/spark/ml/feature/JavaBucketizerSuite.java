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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaBucketizerSuite extends SharedSparkSession {

  @Test
  public void bucketizerTest() {
    double[] splits = {-0.5, 0.0, 0.5};

    StructType schema = new StructType(new StructField[]{
      new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
    });
    Dataset<Row> dataset = spark.createDataFrame(
      Arrays.asList(
        RowFactory.create(-0.5),
        RowFactory.create(-0.3),
        RowFactory.create(0.0),
        RowFactory.create(0.2)),
      schema);

    Bucketizer bucketizer = new Bucketizer()
      .setInputCol("feature")
      .setOutputCol("result")
      .setSplits(splits);

    List<Row> result = bucketizer.transform(dataset).select("result").collectAsList();

    for (Row r : result) {
      double index = r.getDouble(0);
      Assert.assertTrue((index >= 0) && (index <= 1));
    }
  }
}
