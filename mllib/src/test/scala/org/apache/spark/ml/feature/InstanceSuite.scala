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

  test("InstanceBlock: blokify with max memory usage") {
    val instance1 = Instance(19.0, 2.0, Vectors.dense(1.0, 7.0))
    val instance2 = Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse)
    val instances = Seq(instance1, instance2)

    val blocks = InstanceBlock
      .blokifyWithMaxMemUsage(Iterator.apply(instance1, instance2), 128).toArray
    require(blocks.length == 1)
    val block = blocks.head
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

    // instances larger than maxMemUsage
    val denseInstance = Instance(-1.0, 2.0, Vectors.dense(Array.fill(1000)(1.0)))
    InstanceBlock.blokifyWithMaxMemUsage(Iterator.single(denseInstance), 64).size
    InstanceBlock.blokifyWithMaxMemUsage(Iterator.fill(10)(denseInstance), 64).size

    // different numFeatures
    intercept[IllegalArgumentException] {
      InstanceBlock.blokifyWithMaxMemUsage(Iterator.apply(instance1, denseInstance), 64).size
    }

    // nnz = 10
    val sparseInstance = Instance(-2.0, 3.0,
      Vectors.sparse(1000, Array.range(0, 1000, 100), Array.fill(10)(0.1)))

    // normally, memory usage of a block does not exceed maxMemUsage too much
    val maxMemUsage = 1 << 18
    val mixedIter = Iterator.fill(100)(denseInstance) ++
      Iterator.fill(1000)(sparseInstance) ++
      Iterator.fill(10)(denseInstance) ++
      Iterator.fill(10)(sparseInstance) ++
      Iterator.fill(100)(denseInstance) ++
      Iterator.fill(100)(sparseInstance)
    InstanceBlock.blokifyWithMaxMemUsage(mixedIter, maxMemUsage)
      .foreach { block =>
        val doubleBytes = java.lang.Double.BYTES
        val arrayHeader = 12L
        val blockMemUsage = block.matrix.getSizeInBytes +
          (block.labels.length + block.weights.length) * doubleBytes + arrayHeader * 2
        require(blockMemUsage < maxMemUsage * 1.05)
      }
  }
}
