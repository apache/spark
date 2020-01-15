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

package org.apache.spark.mllib.clustering;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaGaussianMixtureSuite extends SharedSparkSession {

  @Test
  public void runGaussianMixture() {
    List<Vector> points = Arrays.asList(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    );

    JavaRDD<Vector> data = jsc.parallelize(points, 2);
    GaussianMixtureModel model = new GaussianMixture().setK(2).setMaxIterations(1).setSeed(1234)
      .run(data);
    assertEquals(2, model.gaussians().length);
    JavaRDD<Integer> predictions = model.predict(data);
    predictions.first();
  }
}
