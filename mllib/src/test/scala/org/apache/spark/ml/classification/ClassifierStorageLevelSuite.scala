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

package org.apache.spark.ml.classification

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasIntermediateStorageLevel
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLWritable}
import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockUpdated}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

class ClassifierStorageLevelSuite
  extends QueryTest with SharedSparkSession with DefaultReadWriteTest {

  import testImplicits._

  namedGridTest("intermediateStorageLevel")(
    Map[String, Estimator[_] with HasIntermediateStorageLevel with MLWritable](
      "LogisticRegression" -> new LogisticRegression().setMaxIter(2),
      "LinearSVC" -> new LinearSVC().setMaxIter(2),
      "FMClassifier" -> new FMClassifier().setMaxIter(2).setSeed(42),
      "MultilayerPerceptronClassifier" -> new MultilayerPerceptronClassifier()
        .setLayers(Array(2, 3, 2)).setMaxIter(2).setSeed(42))) { classifier =>
    assert(classifier.getIntermediateStorageLevel === "MEMORY_AND_DISK")
    Seq("NONE", "invalid").foreach { level =>
      intercept[IllegalArgumentException] {
        classifier.set(classifier.intermediateStorageLevel, level)
      }
    }
    classifier.set(classifier.intermediateStorageLevel, "DISK_ONLY")
    assert(classifier.copy(ParamMap.empty)
      .getOrDefault(classifier.intermediateStorageLevel) === "DISK_ONLY")
    testDefaultReadWrite(classifier)

    val dataset = Seq(
      (0.0, Vectors.dense(0.0, 1.0)),
      (1.0, Vectors.dense(1.0, 0.0)),
      (0.0, Vectors.dense(0.0, 2.0)),
      (1.0, Vectors.dense(2.0, 0.0))).toDF("label", "features")
    val storageLevels = new ConcurrentLinkedQueue[StorageLevel]()
    val listener = new SparkListener {
      override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
        val info = event.blockUpdatedInfo
        if (info.blockId.isInstanceOf[RDDBlockId] && info.storageLevel != StorageLevel.NONE) {
          storageLevels.add(info.storageLevel)
        }
      }
    }
    val sc = spark.sparkContext
    sc.addSparkListener(listener)
    try {
      val model = classifier.fit(dataset)
      assert(model.asInstanceOf[HasIntermediateStorageLevel]
        .getIntermediateStorageLevel === "DISK_ONLY")
      sc.listenerBus.waitUntilEmpty(10000)
      assert(!storageLevels.isEmpty)
      assert(storageLevels.asScala.forall(_ == StorageLevel.DISK_ONLY))
    } finally {
      sc.removeSparkListener(listener)
    }
  }
}
