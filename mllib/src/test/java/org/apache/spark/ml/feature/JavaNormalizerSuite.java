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

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaNormalizerSuite extends SharedSparkSession {

  @Test
  public void normalizer() {
    // The tests are to check Java compatibility.
    JavaRDD<VectorIndexerSuite.FeatureData> points = jsc.parallelize(Arrays.asList(
      new VectorIndexerSuite.FeatureData(Vectors.dense(0.0, -2.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 3.0)),
      new VectorIndexerSuite.FeatureData(Vectors.dense(1.0, 4.0))
    ));
    Dataset<Row> dataFrame = spark.createDataFrame(points, VectorIndexerSuite.FeatureData.class);
    Normalizer normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures");

    // Normalize each Vector using $L^2$ norm.
    Dataset<Row> l2NormData = normalizer.transform(dataFrame, normalizer.p().w(2));
    l2NormData.count();

    // Normalize each Vector using $L^\infty$ norm.
    Dataset<Row> lInfNormData =
      normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
    lInfNormData.count();
  }
}
