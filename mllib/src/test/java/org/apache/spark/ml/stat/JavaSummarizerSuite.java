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

package org.apache.spark.ml.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

public class JavaSummarizerSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    List<LabeledPoint> points = new ArrayList<>();
    points.add(new LabeledPoint(0.0, Vectors.dense(1.0, 2.0)));
    points.add(new LabeledPoint(0.0, Vectors.dense(3.0, 4.0)));

    dataset = spark.createDataFrame(jsc.parallelize(points, 2), LabeledPoint.class);
  }

  @Test
  public void testSummarizer() {
    dataset.select(col("features"));
    Row result = dataset
      .select(Summarizer.metrics("mean", "max", "count").summary(col("features")))
      .first().getStruct(0);
    Vector meanVec = result.getAs("mean");
    Vector maxVec = result.getAs("max");
    long count = result.getAs("count");

    assertEquals(2L, count);
    assertArrayEquals(new double[]{2.0, 3.0}, meanVec.toArray(), 0.0);
    assertArrayEquals(new double[]{3.0, 4.0}, maxVec.toArray(), 0.0);
  }
}
