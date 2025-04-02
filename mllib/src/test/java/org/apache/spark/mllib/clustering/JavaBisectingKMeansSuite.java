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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaBisectingKMeansSuite extends SharedSparkSession {

  @Test
  public void twoDimensionalData() {
    JavaRDD<Vector> points = jsc.parallelize(Arrays.asList(
      Vectors.dense(4, -1),
      Vectors.dense(4, 1),
      Vectors.sparse(2, new int[]{0}, new double[]{1.0})
    ), 2);

    BisectingKMeans bkm = new BisectingKMeans()
      .setK(4)
      .setMaxIterations(2)
      .setSeed(1L);
    BisectingKMeansModel model = bkm.run(points);
    Assertions.assertEquals(3, model.k());
    Assertions.assertArrayEquals(new double[]{3.0, 0.0}, model.root().center().toArray(), 1e-12);
    for (ClusteringTreeNode child : model.root().children()) {
      double[] center = child.center().toArray();
      if (center[0] > 2) {
        Assertions.assertEquals(2, child.size());
        Assertions.assertArrayEquals(new double[]{4.0, 0.0}, center, 1e-12);
      } else {
        Assertions.assertEquals(1, child.size());
        Assertions.assertArrayEquals(new double[]{1.0, 0.0}, center, 1e-12);
      }
    }
  }
}
