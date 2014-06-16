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

package org.apache.spark.rdd

import scala.collection.immutable.LongMap
import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import IndexedRDD.Id
import IndexedRDDFunctions._

trait IndexedRDDOps[
    @specialized(Long, Int, Double) V,
    P[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, P],
    Self[X] <: IndexedRDDBase[X, P] with IndexedRDDOps[X, P, Self]]
  extends RDD[(Id, V)] {

  implicit def vTag: ClassTag[V]

  implicit def pTag[V2]: ClassTag[P[V2]]

  def withPartitionsRDD[V2: ClassTag](partitionsRDD: RDD[P[V2]]): Self[V2]

  def self: Self[V]

  /**
   * (1) copies the base data structure, interrupting the chain of rollback logs. (2) removes deleted entries
   */
  def reindex(): Self[V] = withPartitionsRDD(self.partitionsRDD.map(_.reindex()))

  /**
   * Gets the value corresponding to key k, if any. Only runs one task.
   */
  def get(k: Id): Option[V] = multiget(Array(k)).get(k)

  /**
   * Gets the values corresponding to keys ks. Runs at most one task per partition.  Implemented by
   * looking up the partition for each k and getting only the relevant values from each partition.
   */
  def multiget(ks: Array[Id]): Map[Id, V] = {
    val ksByPartition = ks.groupBy(k => self.partitioner.get.getPartition(k))
    val partitions = ksByPartition.keys.toSeq
    def unionMaps(maps: TraversableOnce[LongMap[V]]): LongMap[V] = {
      maps.foldLeft(LongMap.empty[V]) {
        (accum, map) => accum.unionWith(map, (id, a, b) => a)
      }
    }
    // TODO: avoid sending all keys to all partitions, maybe by creating and zipping an RDD of keys
    val results: Array[Array[(Id, V)]] = self.context.runJob(self.partitionsRDD,
      (context: TaskContext, partIter: Iterator[P[V]]) => {
        if (partIter.hasNext && ksByPartition.contains(context.partitionId)) {
          val part = partIter.next()
          val ksForPartition = ksByPartition.get(context.partitionId).get
          part.multiget(ksForPartition).toArray
        } else {
          Array.empty
        }
      }, partitions, allowLocal = true)
    results.flatten.toMap
  }

  /**
   * Unconditionally updates the key k to have value v, returning a new IndexedRDD.
   * Implemented in terms of multiput.
   */
  def put(k: Id, v: V): Self[V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values, running `merge` on old
   * and new values if necessary.  Implemented by looking up the partition for each k and sending
   * only the relevant values to each partition.
   */
  def multiput(kvs: Map[Id, V]): Self[V] = multiput(kvs, (id, a, b) => b)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary.  Implemented by looking up the partition for each k and sending only the relevant
   * values to each partition.
   */
  def multiput(kvs: Map[Id, V], merge: (Id, V, V) => V): Self[V] = {
    val updates = self.context.parallelize(kvs.toSeq).partitionBy(self.partitioner.get)
    zipPartitionsWithOther(updates)(new MultiputZipper(merge))
  }

  /**
   * Applies a function to each partition of this IndexedRDD.
   */
  protected def mapIndexedRDDPartitions[V2: ClassTag](
      f: P[V] => P[V2]): Self[V2] = {
    val newPartitionsRDD = self.partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    withPartitionsRDD(newPartitionsRDD)
  }

  /**
   * Applies a function to corresponding partitions of `this` and `other`.
   */
  protected def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag](other: Self[V2])
      (f: ZipPartitionsFunction[V2, V3]): Self[V3] = {
    assert(self.partitioner == other.partitioner)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    withPartitionsRDD(newPartitionsRDD)
  }

  /**
   * Applies a function to corresponding partitions of `this` and `other`.
   */
  protected def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag](other: RDD[(Id, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]): Self[V3] = {
    val partitioned = other.partitionWithSerializer(self.partitioner.get)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(partitioned, true)(f)
    withPartitionsRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original RDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(Id, V)]`
   * interface
   */
  override def filter(pred: Tuple2[Id, V] => Boolean): Self[V] =
    this.mapIndexedRDDPartitions(_.filter(Function.untupled(pred)))

  /**
   * Maps each value, preserving the index.
   *
   * @tparam V2 the new value type
   * @param f the function applied to each value
   * @return a new IndexedRDD with values obtained by applying `f` to each of the entries in the
   * original IndexedRDD
   */
  def mapValues[V2: ClassTag](f: V => V2): Self[V2] =
    this.mapIndexedRDDPartitions(_.map((vid, attr) => f(attr)))

  /**
   * Maps each value, additionally supplying the vertex ID.
   *
   * @tparam V2 the type returned by the map function
   *
   * @param f the function applied to each ID-value pair in the RDD
   * @return a new IndexedRDD with values obtained by applying `f` to each of the entries in the
   * original IndexedRDD.  The resulting IndexedRDD retains the same index.
   */
  def mapValues[V2: ClassTag](f: (Id, V) => V2): Self[V2] =
    this.mapIndexedRDDPartitions(_.map(f))

  /**
   * Intersects the key sets of `this` and `other` and hides elements with identical values in the
   * two RDDs; for values that are different, keeps the values from `other`.
   */
  def diff(other: Self[V]): Self[V] =
    this.zipIndexedRDDPartitions(other)(new DiffZipper)

  def join[U: ClassTag]
      (other: RDD[(Id, U)])(f: (Id, V, U) => V): Self[V] = other match {
    case other: Self[U] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new JoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
  }

  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: RDD[(Id, V2)])(f: (Id, V, Option[V2]) => V3): Self[V3] = other match {
    case other: Self[V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new LeftJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherLeftJoinZipper(f))
  }

  def innerJoin[V2: ClassTag, V3: ClassTag](other: RDD[(Id, V2)])
      (f: (Id, V, V2) => V3): Self[V3] = other match {
    case other: Self[V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new InnerJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherInnerJoinZipper(f))
  }

  def createUsingIndex[V2: ClassTag](
      messages: RDD[(Id, V2)]): Self[V2] = {
    this.zipPartitionsWithOther(messages)(new CreateUsingIndexZipper)
  }

  def aggregateUsingIndex[V2: ClassTag](
      messages: RDD[(Id, V2)], reduceFunc: (V2, V2) => V2): Self[V2] = {
    this.zipPartitionsWithOther(messages)(new AggregateUsingIndexZipper(reduceFunc))
  }

  private type ZipPartitionsFunction[V2, V3] =
    Function2[Iterator[P[V]], Iterator[P[V2]], Iterator[P[V3]]]

  private type OtherZipPartitionsFunction[V2, V3] =
    Function2[Iterator[P[V]], Iterator[(Id, V2)], Iterator[P[V3]]]

  private class MultiputZipper(merge: (Id, V, V) => V)
      extends OtherZipPartitionsFunction[V, V] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, V)]): Iterator[P[V]] = {
      val thisPart = thisIter.next()
      val updates = otherIter.toSeq
      Iterator(thisPart.multiput(updates, merge))
    }
  }

  private class DiffZipper extends ZipPartitionsFunction[V, V] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[P[V]]): Iterator[P[V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
  }

  private class JoinZipper[U: ClassTag](f: (Id, V, U) => V)
      extends ZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[P[U]]): Iterator[P[V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.join(otherPart)(f))
    }
  }

  private class OtherJoinZipper[U: ClassTag](f: (Id, V, U) => V)
      extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, U)]): Iterator[P[V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.join(otherIter)(f))
    }
  }

  private class LeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (Id, V, Option[V2]) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[P[V2]]): Iterator[P[V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
  }

  private class OtherLeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (Id, V, Option[V2]) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, V2)]): Iterator[P[V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.leftJoin(otherIter)(f))
    }
  }

  private class InnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (Id, V, V2) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[P[V2]]): Iterator[P[V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
  }

  private class OtherInnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (Id, V, V2) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, V2)]): Iterator[P[V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.innerJoin(otherIter)(f))
    }
  }

  private class CreateUsingIndexZipper[V2: ClassTag]
      extends OtherZipPartitionsFunction[V2, V2] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, V2)]): Iterator[P[V2]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.createUsingIndex(otherIter))
    }
  }

  private class AggregateUsingIndexZipper[V2: ClassTag](reduceFunc: (V2, V2) => V2)
      extends OtherZipPartitionsFunction[V2, V2] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, V2)]): Iterator[P[V2]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.aggregateUsingIndex(otherIter, reduceFunc))
    }
  }
}

private[spark]
class IndexedRDDFunctions[V: ClassTag](self: RDD[(Id, V)]) {
  def partitionWithSerializer(partitioner: Partitioner): RDD[(Id, V)] = {
    val rdd =
      if (self.partitioner == partitioner) self
      else new ShuffledRDD[Id, V, (Id, V)](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    // if (classTag[V] == ClassTag.Int) {
    //   rdd.setSerializer(new IntAggMsgSerializer)
    // } else if (classTag[V] == ClassTag.Long) {
    //   rdd.setSerializer(new LongAggMsgSerializer)
    // } else if (classTag[V] == ClassTag.Double) {
    //   rdd.setSerializer(new DoubleAggMsgSerializer)
    // }
    rdd
  }
}

private[spark]
object IndexedRDDFunctions {
  import scala.language.implicitConversions

  implicit def rdd2IndexedRDDFunctions[V: ClassTag](rdd: RDD[(Id, V)]): IndexedRDDFunctions[V] = {
    new IndexedRDDFunctions(rdd)
  }
}
