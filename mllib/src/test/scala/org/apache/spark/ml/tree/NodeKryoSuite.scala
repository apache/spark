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

package org.apache.spark.ml.tree

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo.KRYO_REGISTRATION_REQUIRED
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.serializer.KryoSerializer

class NodeKryoSuite extends SparkFunSuite {

  gridTest("tree nodes round-trip with strict Kryo registration")(Seq("gini", "entropy")) {
    impurity =>
      val conf = new SparkConf(false).set(KRYO_REGISTRATION_REQUIRED, true)
      val serializer = new KryoSerializer(conf).newInstance()
      val splits: Seq[Split] = Seq(new ContinuousSplit(0, 0.5)) ++
        Seq(0, 1, 2, 3, 4, 5, 6, 7, 12).map { size =>
          new CategoricalSplit(1, Array.tabulate(size)(_.toDouble), 12)
        }
      val roots: Array[Node] = splits.map { split =>
        val leftStats = ImpurityCalculator.getCalculator(impurity, Array(3.0, 0.0, 0.0), 3L)
        val rightStats = ImpurityCalculator.getCalculator(impurity, Array(0.0, 0.0, 3.0), 3L)
        val parentStats = ImpurityCalculator.getCalculator(impurity, Array(3.0, 0.0, 3.0), 6L)
        new InternalNode(0.0, parentStats.calculate(), 1.0,
          new LeafNode(0.0, 0.0, leftStats, 0), new LeafNode(2.0, 0.0, rightStats, 1),
          split, parentStats)
      }.toArray
      val features = for (continuous <- Seq(0.0, 0.5, 1.0); category <- 0 until 12) yield {
        Vectors.dense(continuous, category.toDouble)
      }

      val decoded = serializer.deserialize[Array[Node]](serializer.serialize(roots))
      assert(decoded.length == roots.length)
      for ((original, restored) <- roots.zip(decoded); vector <- features) {
        val originalLeaf = original.predictImpl(vector)
        val restoredLeaf = restored.predictImpl(vector)
        assert(originalLeaf.prediction == restoredLeaf.prediction)
        assert(originalLeaf.leafIndex == restoredLeaf.leafIndex)
        assert(originalLeaf.impurityStats.stats.sameElements(restoredLeaf.impurityStats.stats))
      }
  }
}
