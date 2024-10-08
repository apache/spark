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

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaTargetEncoderSuite extends SharedSparkSession {

  @Test
  public void testTargetEncoderBinary() {

    List<Row> data = Arrays.asList(
            RowFactory.create((short)0, 3, 5.0, 0.0, 1.0/3, 0.0, 1.0/3),
            RowFactory.create((short)1, 4, 5.0, 1.0, 2.0/3, 1.0, 1.0/3),
            RowFactory.create((short)2, 3, 5.0, 0.0, 1.0/3, 0.0, 1.0/3),
            RowFactory.create((short)0, 4, 6.0, 1.0, 1.0/3, 1.0, 2.0/3),
            RowFactory.create((short)1, 3, 6.0, 0.0, 2.0/3, 0.0, 2.0/3),
            RowFactory.create((short)2, 4, 6.0, 1.0, 1.0/3, 1.0, 2.0/3),
            RowFactory.create((short)0, 3, 7.0, 0.0, 1.0/3, 0.0, 0.0),
            RowFactory.create((short)1, 4, 8.0, 1.0, 2.0/3, 1.0, 1.0),
            RowFactory.create((short)2, 3, null, 0.0, 1.0/3, 0.0, 0.0));
    StructType schema = createStructType(new StructField[]{
            createStructField("input1", ShortType, true),
            createStructField("input2", IntegerType, true),
            createStructField("input3", DoubleType, true),
            createStructField("label", DoubleType, false),
            createStructField("expected1", DoubleType, false),
            createStructField("expected2", DoubleType, false),
            createStructField("expected3", DoubleType, false)
    });
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    TargetEncoder encoder = new TargetEncoder()
            .setInputCols(new String[]{"input1", "input2", "input3"})
            .setOutputCols(new String[]{"output1", "output2", "output3"})
            .setTargetType("binary");
    TargetEncoderModel model = encoder.fit(dataset);
    Dataset<Row> output = model.transform(dataset);

    Assertions.assertEquals(
            output.select("output1", "output2", "output3").collectAsList(),
            output.select("expected1", "expected2", "expected3").collectAsList());

  }

  @Test
  public void testTargetEncoderContinuous() {

    List<Row> data = Arrays.asList(
            RowFactory.create((short)0, 3, 5.0, 10.0, 40.0, 50.0, 20.0),
            RowFactory.create((short)1, 4, 5.0, 20.0, 50.0, 50.0, 20.0),
            RowFactory.create((short)2, 3, 5.0, 30.0, 60.0, 50.0, 20.0),
            RowFactory.create((short)0, 4, 6.0, 40.0, 40.0, 50.0, 50.0),
            RowFactory.create((short)1, 3, 6.0, 50.0, 50.0, 50.0, 50.0),
            RowFactory.create((short)2, 4, 6.0, 60.0, 60.0, 50.0, 50.0),
            RowFactory.create((short)0, 3, 7.0, 70.0, 40.0, 50.0, 70.0),
            RowFactory.create((short)1, 4, 8.0, 80.0, 50.0, 50.0, 80.0),
            RowFactory.create((short)2, 3, null, 90.0, 60.0, 50.0, 90.0));
    StructType schema = createStructType(new StructField[]{
            createStructField("input1", ShortType, true),
            createStructField("input2", IntegerType, true),
            createStructField("input3", DoubleType, true),
            createStructField("label", DoubleType, false),
            createStructField("expected1", DoubleType, false),
            createStructField("expected2", DoubleType, false),
            createStructField("expected3", DoubleType, false)
    });
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    TargetEncoder encoder = new TargetEncoder()
            .setInputCols(new String[]{"input1", "input2", "input3"})
            .setOutputCols(new String[]{"output1", "output2", "output3"})
            .setTargetType("continuous");
    TargetEncoderModel model = encoder.fit(dataset);
    Dataset<Row> output = model.transform(dataset);

    Assertions.assertEquals(
            output.select("output1", "output2", "output3").collectAsList(),
            output.select("expected1", "expected2", "expected3").collectAsList());

  }

}
