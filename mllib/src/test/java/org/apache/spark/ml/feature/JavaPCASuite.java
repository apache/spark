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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JavaPCASuite extends SharedSparkSession {

  public static class VectorPair implements Serializable {
    private Vector features = Vectors.dense(0.0);
    private Vector expected = Vectors.dense(0.0);

    public void setFeatures(Vector features) {
      this.features = features;
    }

    public Vector getFeatures() {
      return this.features;
    }

    public void setExpected(Vector expected) {
      this.expected = expected;
    }

    public Vector getExpected() {
      return this.expected;
    }
  }

  @Test
  public void testPCA() {
    List<Vector> points = Arrays.asList(
      Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0}),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    );
    JavaRDD<Vector> dataRDD = jsc.parallelize(points, 2);

    RowMatrix mat = new RowMatrix(dataRDD.map(
        (Vector vector) -> (org.apache.spark.mllib.linalg.Vector) new DenseVector(vector.toArray())
    ).rdd());

    Matrix pc = mat.computePrincipalComponents(3);

    mat.multiply(pc).rows().toJavaRDD();

    JavaRDD<Vector> expected = mat.multiply(pc).rows().toJavaRDD()
        .map(org.apache.spark.mllib.linalg.Vector::asML);

    JavaRDD<VectorPair> featuresExpected = dataRDD.zip(expected).map(pair -> {
      VectorPair featuresExpected1 = new VectorPair();
      featuresExpected1.setFeatures(pair._1());
      featuresExpected1.setExpected(pair._2());
      return featuresExpected1;
    });

    Dataset<Row> df = spark.createDataFrame(featuresExpected, VectorPair.class);
    PCAModel pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(3)
      .fit(df);
    List<Row> result = pca.transform(df).select("pca_features", "expected").toJavaRDD().collect();
    for (Row r : result) {
      Vector calculatedVector = (Vector) r.get(0);
      Vector expectedVector = (Vector) r.get(1);
      for (int i = 0; i < calculatedVector.size(); i++) {
        Assert.assertEquals(calculatedVector.apply(i), expectedVector.apply(i), 1.0e-8);
      }
    }
  }
}
