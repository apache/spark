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

package org.apache.spark.ml.clustering;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaKMeansSuite extends SharedSparkSession {

  private transient int k = 5;
  private transient Dataset<Row> dataset;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    super.setUp();
    dataset = KMeansSuite.generateKMeansData(spark, 50, 3, k);
  }

  @Test
  public void fitAndTransform() {
    KMeans kmeans = new KMeans().setK(k).setSeed(1);
    KMeansModel model = kmeans.fit(dataset);

    Vector[] centers = model.clusterCenters();
    assertEquals(k, centers.length);

    Dataset<Row> transformed = model.transform(dataset);
    List<String> columns = Arrays.asList(transformed.columns());
    List<String> expectedColumns = Arrays.asList("features", "prediction");
    for (String column : expectedColumns) {
      assertTrue(columns.contains(column));
    }
  }
}
