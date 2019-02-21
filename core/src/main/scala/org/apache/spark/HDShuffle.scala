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

package org.apache.spark

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.spark.TensorConnectionType.TensorConnectionType
import org.apache.spark.rdd.{RDD, ShuffledRDDPartition}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}

object TensorConnectionType extends Enumeration {
  type TensorConnectionType = Value
  val None, Resharp, Partition, Aggregate = Value
}

object Tensor {
  def create(partitionLength: Int): Tensor = {
    new Tensor(Array(partitionLength), null, TensorConnectionType.None)
  }
}

class Tensor(val dimensionLengths: Array[Int],
             val parent: Tensor,
             val connectionType: TensorConnectionType,
             val arg: Option[Any] = None) extends Serializable {
  private def resharp(lengths: Array[Int]): Tensor = {
    new Tensor(lengths, this, TensorConnectionType.Resharp)
  }

  private def resharp(limit: Int): Tensor = {
    var dimensions = new ListBuffer[Int]
    var n = dimensionLengths.product

    while (n > limit) {
      dimensions += limit
      n /= limit
    }

    dimensions += n
    resharp(dimensions.toArray)
  }

  // resharp the tensor to the giving rank if current rank less than the given rank by append 1
  private def resharpToRank(rank: Int) = {
    if (rank > dimensionLengths.length) {
      resharp(dimensionLengths.padTo(rank, 1))
    }
    else {
      this
    }
  }

  private def partition(num: Int): Tensor = {
    val dimensions = dimensionLengths.toBuffer
    dimensions.prepend(num)
    new Tensor(dimensions.toArray, this, TensorConnectionType.Partition)
  }

  private def aggregate(index: Int): Tensor = {
    val dimensions = dimensionLengths.toBuffer
    dimensions.remove(index + 1)
    new Tensor(dimensions.toArray, this, TensorConnectionType.Aggregate, Some(index + 1))
  }

  // given a index check weather it can connect to sourceIndex
  private def hasConnection(indices: Array[Int], sourceIndex: Array[Int]): Boolean = {
    val parentIndexes = sourceIndex.toBuffer
    val aggIndex: Int = arg.get.asInstanceOf[Int]
    parentIndexes.remove(aggIndex)
    indices.sameElements(parentIndexes)
  }

  @transient private var unShuffledDimensions_ : Int = _

  private def getUnShuffledDimensionLength(partitionDims: Array[Int]): Int = {
    if (unShuffledDimensions_ == 0) {
      val aggIndex: Int = arg.get.asInstanceOf[Int]
      unShuffledDimensions_ = partitionDims.drop(aggIndex).product
    }
    unShuffledDimensions_
  }

  // this operator will build a plan to shuffle the original tensor
  // to a tensor with the [target] number of partitions and honor the limitation
  def shuffle(target: Int, limit: Int) : Tensor = {
    val targetTensor = Tensor.create(target).resharp(limit)
    var result = resharp(limit).resharpToRank(targetTensor.dimensionLengths.length)
    for (d <- targetTensor.dimensionLengths.zipWithIndex) {
      result = result.partition(d._1).aggregate(d._2)
    }
    result
  }

  // return a flat length (length in one dimension)
  def getFlatLength: Int = {
    dimensionLengths.product
  }

  def getPartition(flatPartitionIndex: Int, partitionDims: Array[Int]): Int = {
    (flatPartitionIndex / getUnShuffledDimensionLength(partitionDims)) % parent.dimensionLengths(0)
  }

  // given a 1 dimension index, convert it to the indices in N dimension space according
  // to current dimension lengths
  def convertToNDimension(index: Int): Array[Int] = {
    var dimensions = new ListBuffer[Int]
    var n = index
    for (l <- dimensionLengths) {
      var i = n % l
      dimensions += i
      n /= l
    }
    dimensions.toArray
  }

  def shouldRead(reduceId: Int, mapId: Int, bucketId: Int): Boolean = {
    // [2, 4] => [3,5]
    // [2,4] => [2,4, 1..5] => [2,5] => [2,5,1..3] => [5, 3]
    val sourceIndices = parent.parent.convertToNDimension(mapId).toBuffer
    sourceIndices.prepend(bucketId)
    val currentIndices = convertToNDimension(reduceId)
    hasConnection(currentIndices, sourceIndices.toArray)
  }
}

class HDShuffleRDD[K: ClassTag, V: ClassTag, C: ClassTag]
(var dependency: ShuffleDependency[K, V, C])
  extends RDD[(K, C)](dependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](dependency.partitioner.numPartitions)(i => new
        ShuffledRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    // val shuffledRowPartition = split.asInstanceOf[ShuffledRDDPartition]
    // The range of pre-shuffle partitions that we are fetching at here is
    // [startPreShufflePartitionIndex, endPreShufflePartitionIndex - 1].
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    val reader =
    SparkEnv.get.shuffleManager.getReader(
      dependency.shuffleHandle,
      split.index,
      split.index + 1,
      context,
      metrics)
    reader.read().asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}

// given a ShuffleDependency, convert to HDShuffle
object HDShuffleConverter {
  val limit: Int = 4

  // given a ShuffleDependency, we can find the source partition
  // and target partition and honor the partition length limitation
  // and covert the original plan (rdd ==(ShuffleDependency)==ShuffedRdd) to something like
  // (rdd ==(ShuffleDependency)==HDShuffleRDD==(ShuffleDependency)==
  // HDShuffleRDD==(ShuffleDependency)==ShuffedRdd)
  // to do that we leverage a multiple dimension array,
  // called tensor, we call the shuffle method in the tensor to
  // convert the dimension from source to target, take a example (48 => 32)
  // tensor(48)=[resharp]=>tensor(4,4,3)=[partition]
  // =>tensor(1..4,4,4,3)=[aggregate]=>tensor([4],4,3)
  // =[partition]=>tensor(1..4,4,[4],3)=[aggregate]
  // =>tensor([4,4],3)=[partition]=>tensor(1..2,4,4,[3])
  // =[aggregate]=>tensor([2,4,4])
  def doConvert[K: ClassTag, V: ClassTag, C: ClassTag]
  (inputDep: ShuffleDependency[K, V, C]): ShuffleDependency[K, V, C] = {
    if (!SparkEnv.get.conf.getBoolean("spark.shuffle.HdShuffle.enable", true)) {
      return inputDep
    }

    if (inputDep.partitioner.numPartitions > limit && inputDep.rdd.getNumPartitions > limit) {
      // create a hd tensor
      val finalTensor = Tensor.create(inputDep.rdd.getNumPartitions)
        .shuffle(inputDep.partitioner.numPartitions, limit)

      // Aggregate tensor is the fina tensor
      val aggregateTensors = new ListBuffer[Tensor]

      var tensor = finalTensor
      while(tensor != null) {
        if (tensor.connectionType == TensorConnectionType.Aggregate) {
          aggregateTensors.prepend(tensor)
        }
        tensor = tensor.parent
      }

      var preDep = inputDep
      var preRDD = inputDep.rdd
      for (tensor <- aggregateTensors) {

        preDep = new ShuffleDependency[K, V, C](
          preRDD,
          new HDPartitioner(inputDep.partitioner, tensor, limit),
          inputDep.serializer,
          inputDep.keyOrdering,
          inputDep.aggregator,
          inputDep.mapSideCombine)

        preRDD = new HDShuffleRDD[K, V, C](preDep).asInstanceOf[RDD[Product2[K, V]]]
      }

      preDep
    }
    else {
      inputDep
    }
  }
}

class HDPartitioner(origin: Partitioner, aggregateTensor: Tensor, limit: Int) extends Partitioner {

  @transient private var numPartitions_ : Int = _
  @transient private var partitionDims_ : Array[Int] = _

  // this function will called by shuffled RDD, that is the target partition number after shuffle
  def numPartitions: Int = {
    if (numPartitions_ == 0) {
      numPartitions_ = origin.numPartitions
    }
    numPartitions_
  }

  def getPartitionDims: Array[Int] = {
    if (partitionDims_ == null) {
      var dimensions = new ListBuffer[Int]
      var n = numPartitions

      while (n > limit) {
        dimensions += limit
        n /= limit
      }

      dimensions += n
      partitionDims_ = dimensions.toArray
    }
    partitionDims_
  }

  override def getPartition(key: Any): Int = {
    val p = origin.getPartition(key)
    aggregateTensor.getPartition(p, getPartitionDims)
  }

  // get the partition index the given reducer will read
  def getPartitionIndex(reduceIndex: Int): Int = {
    aggregateTensor.convertToNDimension(reduceIndex)(0)
  }

  def filter(input: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
             reducerIndexStart: Int, reducerIndexEnd: Int):
  Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    input.map(in => {
      (in._1, in._2.filter(i => {
        val blockId = i._1.asInstanceOf[ShuffleBlockId]
        aggregateTensor.shouldRead(reducerIndexStart, blockId.mapId, blockId.reduceId)
      }))
    })
  }
}