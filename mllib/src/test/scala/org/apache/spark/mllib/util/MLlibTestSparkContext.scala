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

package org.apache.spark.mllib.util

import java.io.File

import org.scalatest.Suite

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.TempDirectory
import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.util.Utils

trait MLlibTestSparkContext extends TempDirectory { self: Suite =>
  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _
  @transient var checkpointDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder
      .master("local[2]")
      .appName("MLlibUnitTest")
      .getOrCreate()
    sc = spark.sparkContext
    // initialize SessionCatalog here so it has a clean hadoopConf
    spark.sessionState.catalog

    checkpointDir = Utils.createDirectory(tempDir.getCanonicalPath, "checkpoints").toString
    sc.setCheckpointDir(checkpointDir)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(new File(checkpointDir))
      SparkSession.clearActiveSession()
      if (spark != null) {
        spark.stop()
      }
      spark = null
    } finally {
      super.afterAll()
    }
  }

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  private[spark] def standardize(instances: Array[Instance]): Array[Instance] = {
    val (featuresSummarizer, _) =
      Summarizer.getClassificationSummarizers(sc.parallelize(instances))
    val inverseStd = featuresSummarizer.std.toArray
      .map { std => if (std != 0) 1.0 / std else 0.0 }
    val func = StandardScalerModel.getTransformFunc(Array.empty, inverseStd, false, true)
    instances.map { case Instance(label, weight, vec) => Instance(label, weight, func(vec)) }
  }
}
