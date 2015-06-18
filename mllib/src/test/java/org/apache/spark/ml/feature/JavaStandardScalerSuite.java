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

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class JavaStandardScalerSuite {
  private transient JavaSparkContext jsc;
  private transient SQLContext jsql;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaStandardScalerSuite");
    jsql = new SQLContext(jsc);
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void standardScaler() {
    // The tests are to check Java compatibility.
    List<VectorIndexerSuite.FeatureData> points = Lists.newArrayList(
      new VectorIndexerSuite.FeatureData(Vectors.dense(0.0, -2.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 3.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 4.0))
    );
    DataFrame dataFrame = jsql.createDataFrame(jsc.parallelize(points, 2),
      VectorIndexerSuite.FeatureData.class);
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false);

    // Compute summary statistics by fitting the StandardScaler
    StandardScalerModel scalerModel = scaler.fit(dataFrame);

    // Normalize each feature to have unit standard deviation.
    DataFrame scaledData = scalerModel.transform(dataFrame);
    scaledData.count();
  }
}
