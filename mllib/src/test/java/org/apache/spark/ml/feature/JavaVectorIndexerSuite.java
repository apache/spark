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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorIndexerSuite.FeatureData;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class JavaVectorIndexerSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaVectorIndexerSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void vectorIndexerAPI() {
    // The tests are to check Java compatibility.
    List<FeatureData> points = Arrays.asList(
      new FeatureData(Vectors.dense(0.0, -2.0)),
      new FeatureData(Vectors.dense(1.0, 3.0)),
      new FeatureData(Vectors.dense(1.0, 4.0))
    );
    SQLContext sqlContext = new SQLContext(sc);
    DataFrame data = sqlContext.createDataFrame(sc.parallelize(points, 2), FeatureData.class);
    VectorIndexer indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(2);
    VectorIndexerModel model = indexer.fit(data);
    Assert.assertEquals(model.numFeatures(), 2);
    Map<Integer, Map<Double, Integer>> categoryMaps = model.javaCategoryMaps();
    Assert.assertEquals(categoryMaps.size(), 1);
    DataFrame indexedData = model.transform(data);
  }
}
