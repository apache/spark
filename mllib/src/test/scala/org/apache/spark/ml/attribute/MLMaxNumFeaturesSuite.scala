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

// This suite lives in the ml.attribute package so it can exercise the package-private
// AttributeGroup.fromMetadata; the other checked APIs are private[spark].
package org.apache.spark.ml.attribute

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.MetadataBuilder

class MLMaxNumFeaturesSuite extends SparkFunSuite {

  // spark.sql.ml.maxNumFeatures is a static conf, so it must be set when the session is created.
  private def withCappedSession(cap: Int)(f: SparkSession => Unit): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("MLMaxNumFeaturesSuite")
      .config(StaticSQLConf.ML_MAX_NUM_FEATURES.key, cap.toString)
      .getOrCreate()
    try {
      // If another suite's SparkContext were reused, the static conf would be ignored; fail fast.
      assert(spark.conf.get(StaticSQLConf.ML_MAX_NUM_FEATURES.key) === cap.toString)
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  test("spark.sql.ml.maxNumFeatures caps feature counts and vector sizes") {
    withCappedSession(10) { spark =>
      val sc = spark.sparkContext

      // libsvm-inferred feature dimension (max index 50 -> 51 > 10)
      val big = sc.parallelize(Seq((1.0, Array(0, 50), Array(1.0, 1.0))))
      assert(intercept[IllegalArgumentException](MLUtils.computeNumFeatures(big))
        .getMessage.contains("maxNumFeatures"))

      // attribute-metadata count
      val meta = new MetadataBuilder().putLong(AttributeKeys.NUM_ATTRIBUTES, 100L).build()
      assert(intercept[IllegalArgumentException](AttributeGroup.fromMetadata(meta, "g"))
        .getMessage.contains("maxNumFeatures"))

      // stored sparse-vector size, both ml.linalg and mllib.linalg
      val mlUDT = new org.apache.spark.ml.linalg.VectorUDT
      val mlVec = org.apache.spark.ml.linalg.Vectors.sparse(1000, Array(0), Array(1.0))
      assert(intercept[IllegalArgumentException](mlUDT.deserialize(mlUDT.serialize(mlVec)))
        .getMessage.contains("maxNumFeatures"))
      val oldUDT = new org.apache.spark.mllib.linalg.VectorUDT
      val oldVec = org.apache.spark.mllib.linalg.Vectors.sparse(1000, Array(0), Array(1.0))
      assert(intercept[IllegalArgumentException](oldUDT.deserialize(oldUDT.serialize(oldVec)))
        .getMessage.contains("maxNumFeatures"))

      // Within the cap, everything is accepted unchanged.
      val small = sc.parallelize(Seq((1.0, Array(0, 3), Array(1.0, 1.0))))
      assert(MLUtils.computeNumFeatures(small) === 4)
      val smallVec = org.apache.spark.ml.linalg.Vectors.sparse(5, Array(0), Array(1.0))
      assert(mlUDT.deserialize(mlUDT.serialize(smallVec)) === smallVec)
    }
  }
}
