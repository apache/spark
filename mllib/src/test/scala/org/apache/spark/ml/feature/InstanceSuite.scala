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

package org.apache.spark.ml.feature

import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer

class InstanceSuite extends SparkFunSuite{
  test("Kryo class register") {
    val conf = new SparkConf(false)
    conf.set(KRYO_REGISTRATION_REQUIRED, true)

    val ser = new KryoSerializer(conf).newInstance()

    val instance1 = Instance(19.0, 2.0, Vectors.dense(1.0, 7.0))
    val instance2 = Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse)
    Seq(instance1, instance2).foreach { i =>
      val i2 = ser.deserialize[Instance](ser.serialize(i))
      assert(i === i2)
    }

    val oInstance1 = OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0))
    val oInstance2 = OffsetInstance(0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0).toSparse)
    Seq(oInstance1, oInstance2).foreach { o =>
      val o2 = ser.deserialize[OffsetInstance](ser.serialize(o))
      assert(o === o2)
    }

    val block1 = InstanceBlock.fromInstances(Seq(instance1))
    val block2 = InstanceBlock.fromInstances(Seq(instance1, instance2))
    Seq(block1, block2).foreach { o =>
      val o2 = ser.deserialize[InstanceBlock](ser.serialize(o))
      assert(o.labels === o2.labels)
      assert(o.weights === o2.weights)
      assert(o.matrix === o2.matrix)
    }
  }

  test("InstanceBlock: check correctness") {
    val instance1 = Instance(19.0, 2.0, Vectors.dense(1.0, 7.0))
    val instance2 = Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse)
    val instances = Seq(instance1, instance2)

    val block = InstanceBlock.fromInstances(instances)
    assert(block.size === 2)
    assert(block.numFeatures === 2)
    block.instanceIterator.zipWithIndex.foreach {
      case (instance, i) =>
        assert(instance.label === instances(i).label)
        assert(instance.weight === instances(i).weight)
        assert(instance.features.toArray === instances(i).features.toArray)
    }
    Seq(0, 1).foreach { i =>
      val nzIter = block.getNonZeroIter(i)
      val vec = Vectors.sparse(2, nzIter.toSeq)
      assert(vec.toArray === instances(i).features.toArray)
    }
  }

  test("InstanceBlock: memory usage limit") {
    val rng = new Random(123L)
    val instances = Seq.fill(1000) {
      Instance(rng.nextDouble, rng.nextDouble, Vectors.dense(Array.fill(1000)(rng.nextDouble)))
    }

    Seq(1, 2, 3).foreach { maxBlockMemoryInMB =>
      val maxMemoryUsage = maxBlockMemoryInMB * 1024L * 1024L
      val blocks = InstanceBlock.blokifyWithMaxMemoryUsage(
        instances.iterator, maxMemoryUsage).toSeq
      val flatten = blocks.flatMap { block => block.instanceIterator }
      assert(instances.size == flatten.size)
      assert(instances.iterator.zip(flatten.iterator).forall(t => t._1 === t._2))
    }

    Seq(2, 4, 64).foreach { i =>
      val instanceIter = Iterator.single(Instance(rng.nextDouble, rng.nextDouble,
        Vectors.dense(Array.fill(1024 * i)(rng.nextDouble))))
      assert(InstanceBlock.blokifyWithMaxMemoryUsage(instanceIter, 1024L * 1024L).size == 1)
    }

    Seq(128, 256).foreach { i =>
      val instanceIter = Iterator.single(Instance(rng.nextDouble, rng.nextDouble,
        Vectors.dense(Array.fill(1024 * i)(rng.nextDouble))))
      intercept[IllegalArgumentException] {
        InstanceBlock.blokifyWithMaxMemoryUsage(instanceIter, 1024L * 1024L).size
      }
    }
  }
}
