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

public class JavaFrequencyEncoderSuite extends SharedSparkSession {

  // Six rows. In input1, category 0 occurs three times, 2 twice and 1 once; in input2,
  // category 1 occurs three times, 0 twice and 2 once. Expectations are written as x / 6.0
  // to match the arithmetic fit performs.
  private static final List<Row> DATA = Arrays.asList(
    RowFactory.create(0.0, 1.0, 3.0 / 6.0, 3.0 / 6.0, 3.0, 3.0),
    RowFactory.create(1.0, 0.0, 1.0 / 6.0, 2.0 / 6.0, 1.0, 2.0),
    RowFactory.create(2.0, 1.0, 2.0 / 6.0, 3.0 / 6.0, 2.0, 3.0),
    RowFactory.create(0.0, 2.0, 3.0 / 6.0, 1.0 / 6.0, 3.0, 1.0),
    RowFactory.create(0.0, 1.0, 3.0 / 6.0, 3.0 / 6.0, 3.0, 3.0),
    RowFactory.create(2.0, 0.0, 2.0 / 6.0, 2.0 / 6.0, 2.0, 2.0));

  private static final StructType SCHEMA = createStructType(new StructField[]{
    createStructField("input1", DoubleType, true),
    createStructField("input2", DoubleType, true),
    createStructField("expected1", DoubleType, false),
    createStructField("expected2", DoubleType, false),
    createStructField("count1", DoubleType, false),
    createStructField("count2", DoubleType, false)
  });

  @Test
  public void testFrequencyEncoderProportions() {
    Dataset<Row> df = spark.createDataFrame(DATA, SCHEMA);

    FrequencyEncoder encoder = new FrequencyEncoder()
      .setInputCols(new String[]{"input1", "input2"})
      .setOutputCols(new String[]{"output1", "output2"});

    FrequencyEncoderModel model = encoder.fit(df);
    Dataset<Row> encoded = model.transform(df);

    Assertions.assertEquals(6, encoded.count());
    List<Row> rows = encoded.select("output1", "expected1", "output2", "expected2")
      .collectAsList();
    for (Row row : rows) {
      Assertions.assertEquals(row.getDouble(1), row.getDouble(0), 0.0);
      Assertions.assertEquals(row.getDouble(3), row.getDouble(2), 0.0);
    }
  }

  @Test
  public void testFrequencyEncoderRawCounts() {
    Dataset<Row> df = spark.createDataFrame(DATA, SCHEMA);

    FrequencyEncoder encoder = new FrequencyEncoder()
      .setInputCols(new String[]{"input1", "input2"})
      .setOutputCols(new String[]{"output1", "output2"})
      .setNormalize(false);

    FrequencyEncoderModel model = encoder.fit(df);
    Dataset<Row> encoded = model.transform(df);

    List<Row> rows = encoded.select("output1", "count1", "output2", "count2").collectAsList();
    for (Row row : rows) {
      Assertions.assertEquals(row.getDouble(1), row.getDouble(0), 0.0);
      Assertions.assertEquals(row.getDouble(3), row.getDouble(2), 0.0);
    }
  }

  @Test
  public void testFrequencyEncoderUnseenKeep() {
    Dataset<Row> df = spark.createDataFrame(DATA, SCHEMA);

    FrequencyEncoderModel model = new FrequencyEncoder()
      .setInputCol("input1")
      .setOutputCol("output1")
      .setHandleInvalid("keep")
      .fit(df);

    Dataset<Row> unseen = spark.createDataFrame(
      Arrays.asList(RowFactory.create(9.0, 1.0, 0.0, 0.0, 0.0, 0.0)), SCHEMA);

    // Category 9 was never seen, so its observed frequency is zero.
    Assertions.assertEquals(
      0.0, model.transform(unseen).select("output1").head().getDouble(0), 0.0);
  }
}
