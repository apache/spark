/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import com.esotericsoftware.kryo.io.Input
import org.testng.Assert
import org.testng.annotations.Test

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

class CombinerRecordBufferManagerTest {
  val serializer = new KryoSerializer(getConf)

  @Test
  def singlePartition(): Unit = {
    var spillSize = 1
    val record = (1, "123")
    var bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    var spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, 0)

    val partition1 = 1

    bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)

    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    // use large buffer to get size for one record after serialization
    spillSize = 1024 * 1024
    bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)
    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 0)
    val oneRecordSize = bufferManager.filledBytes
    Assert.assertTrue(oneRecordSize > 0)

    spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)

    // use spill size a little more than one record
    spillSize = oneRecordSize + 1
    bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)

    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition1, record)
    Assert.assertEquals(spilledData.size, 1)
    Assert.assertEquals(bufferManager.filledBytes, 0)
    Assert.assertEquals(spilledData(0)._1, partition1)
    val deserializeKeyValuePairs = deserializeData(spilledData(0)._2)
    Assert.assertEquals(deserializeKeyValuePairs.size, 1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._1, record._1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._2, Seq(record._2, record._2))
  }

  @Test
  def twoPartitions(): Unit = {
    val record1 = (1, "123")
    val record2 = (2, "abc")

    val partition1 = 11
    val partition2 = 12

    // use large buffer to get size for one record after serialization
    var spillSize = 1024 * 1024
    var bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)
    var spilledData = bufferManager.addRecord(partition1, record1)
    Assert.assertEquals(spilledData.size, 0)
    val oneRecordSize = bufferManager.filledBytes
    Assert.assertTrue(oneRecordSize > 0)

    // use spill size a little more than one record
    spillSize = oneRecordSize + 1
    bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)

    spilledData = bufferManager.addRecord(partition1, record1)
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition2, record2)
    Assert.assertEquals(spilledData.size, 2)
    Assert.assertEquals(bufferManager.filledBytes, 0)
    Assert.assertEquals(spilledData(0)._1, partition1)
    var deserializeKeyValuePairs = deserializeData(spilledData(0)._2)
    Assert.assertEquals(deserializeKeyValuePairs.size, 1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._1, record1._1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._2, Seq(record1._2))
    Assert.assertEquals(spilledData(1)._1, partition2)
    deserializeKeyValuePairs = deserializeData(spilledData(1)._2)
    Assert.assertEquals(deserializeKeyValuePairs.size, 1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._1, record2._1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._2, Seq(record2._2))

    // use very spill size
    spillSize = 1024 * 1024 * 1024
    bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)

    spilledData = bufferManager.addRecord(partition1, record1)
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.addRecord(partition2, record2)
    Assert.assertEquals(spilledData.size, 0)
    Assert.assertTrue(bufferManager.filledBytes > 0)

    spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, 2)
    Assert.assertEquals(bufferManager.filledBytes, 0)
    Assert.assertEquals(spilledData(0)._1, partition1)
    deserializeKeyValuePairs = deserializeData(spilledData(0)._2)
    Assert.assertEquals(deserializeKeyValuePairs.size, 1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._1, record1._1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._2, Seq(record1._2))
    Assert.assertEquals(spilledData(1)._1, partition2)
    deserializeKeyValuePairs = deserializeData(spilledData(1)._2)
    Assert.assertEquals(deserializeKeyValuePairs.size, 1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._1, record2._1)
    Assert.assertEquals(deserializeKeyValuePairs(0)._2, Seq(record2._2))
  }

  @Test
  def manyPartitions(): Unit = {
    val numPartitions = 100

    val spillSize = 10 * 1024 * 1024
    val bufferManager = new CombinerRecordBufferManager[Any, Any, Seq[Any]](
      createCombiner = v => Seq(v),
      mergeValue = (c, v) => c :+ v,
      serializer = serializer,
      spillSize = spillSize)

    for (partition <- 0 to numPartitions - 1) {
      val key = partition * 2
      val value = key.toString
      bufferManager.addRecord(partition, (key, value))
    }

    for (partition <- numPartitions - 1 to 0 by -1) {
      val key = partition * 2
      val value = (key + 1).toString
      bufferManager.addRecord(partition, (key, value))
    }

    val spilledData = bufferManager.clear()
    Assert.assertEquals(spilledData.size, numPartitions)
    Assert.assertEquals(bufferManager.filledBytes, 0)
    for (i <- 0 to numPartitions - 1) {
      val item = spilledData(i)
      Assert.assertEquals(item._1, i)
      val deserializeKeyValuePairs = deserializeData(item._2)
      Assert.assertEquals(deserializeKeyValuePairs.size, 1)
      Assert.assertEquals(deserializeKeyValuePairs(0)._1, i * 2)
      Assert.assertEquals(deserializeKeyValuePairs(0)._2,
        Seq((i * 2).toString, ((i * 2) + 1).toString))
    }
  }

  private def getConf = {
    new SparkConf().setAppName("testApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  private def deserializeData(data: Array[Byte]): Seq[(Any, Any)] = {
    val input = new Input(data)
    val stream = serializer.newInstance().deserializeStream(input)
    stream.asKeyValueIterator.toList
  }
}
