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

/**
 * Contains functions whose implementations are shared among all variants of IndexedRDD (e.g.,
 * IndexedRDD, VertexRDD). Unlike `IndexedRDDLike`, `IndexedRDDOps` takes a `Self` type parameter,
 * which allows transformation methods on any variant of IndexedRDD to yield a result of the same
 * type.
 *
 * @tparam V the type of the values stored in the IndexedRDD
 * @tparam P the type of the partitions making up the IndexedRDD
 * @tparam Self the type of the implementing container, which must be a variant of IndexedRDD
 */
private[spark] trait IndexedRDDOps[
    @specialized(Long, Int, Double) V,
    P[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, P],
    Self[X] <: IndexedRDDLike[X, P] with IndexedRDDOps[X, P, Self]]
  extends RDD[(Id, V)] {

  /** A generator for ClassTags of the value type V. */
  implicit def vTag: ClassTag[V]

  /** A generator for ClassTags of the partition type P. */
  implicit def pTag[V2]: ClassTag[P[V2]]

  /** Accessor for the IndexedRDD variant that is mixing in this trait. */
  def self: Self[V]

  def withPartitionsRDD[V2: ClassTag](partitionsRDD: RDD[P[V2]]): Self[V2]

  /**
   * Rebuilds the indexes of this IndexedRDD, removing deleted entries. The resulting IndexedRDD
   * will not support efficient joins with the original one.
   */
  def reindex(): Self[V] = withPartitionsRDD(self.partitionsRDD.map(_.reindex()))

  /** Gets the value corresponding to the specified key, if any. */
  def get(k: Id): Option[V] = multiget(Array(k)).get(k)

  /** Gets the values corresponding to the specified keys, if any. */
  def multiget(ks: Array[Id]): Map[Id, V] = {
    val ksByPartition = ks.groupBy(k => self.partitioner.get.getPartition(k))
    val partitions = ksByPartition.keys.toSeq
    def unionMaps(maps: TraversableOnce[LongMap[V]]): LongMap[V] = {
      maps.foldLeft(LongMap.empty[V]) {
        (accum, map) => accum.unionWith(map, (id, a, b) => a)
      }
    }
    // TODO: avoid sending all keys to all partitions by creating and zipping an RDD of keys
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
   * Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
   * that reflects the modification.
   */
  def put(k: Id, v: V): Self[V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values. Returns a new
   * IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[Id, V]): Self[V] = multiput(kvs, (id, a, b) => b)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[Id, V], merge: (Id, V, V) => V): Self[V] = {
    val updates = self.context.parallelize(kvs.toSeq).partitionBy(self.partitioner.get)
    zipPartitionsWithOther(updates)(new MultiputZipper(merge))
  }

  /** Deletes the specified keys. Returns a new IndexedRDD that reflects the deletions. */
  def delete(ks: Array[Id]): Self[V] = {
    val deletions = self.context.parallelize(ks.map(k => (k, ()))).partitionBy(self.partitioner.get)
    zipPartitionsWithOther(deletions)(new DeleteZipper)
  }

  /**
   * Applies a function to each partition of this IndexedRDD.
   */
  protected def mapIndexedRDDPartitions[V2: ClassTag](
      f: P[V] => P[V2]): Self[V2] = {
    val newPartitionsRDD = self.partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    withPartitionsRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and `other`. */
  protected def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag](other: Self[V2])
      (f: ZipPartitionsFunction[V2, V3]): Self[V3] = {
    assert(self.partitioner == other.partitioner)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    withPartitionsRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and `other`. */
  protected def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag](other: RDD[(Id, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]): Self[V3] = {
    val partitioned = other.partitionBy(self.partitioner.get)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(partitioned, true)(f)
    withPartitionsRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDD and is implemented using soft deletions.
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

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: RDD[(Id, U)])(f: (Id, V, U) => V): Self[V] = other match {
    case other: Self[U] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new JoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
  }

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: RDD[(Id, V2)])(f: (Id, V, Option[V2]) => V3): Self[V3] = other match {
    case other: Self[V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new LeftJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherLeftJoinZipper(f))
  }

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[V2: ClassTag, V3: ClassTag](other: RDD[(Id, V2)])
      (f: (Id, V, V2) => V3): Self[V3] = other match {
    case other: Self[V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new InnerJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherInnerJoinZipper(f))
  }

  /**
   * Creates a new IndexedRDD with values from `messages` that shares an index with `this`, merging
   * duplicate keys in `messages` arbitrarily.
   */
  def createUsingIndex[V2: ClassTag](
      messages: RDD[(Id, V2)]): Self[V2] = {
    this.zipPartitionsWithOther(messages)(new CreateUsingIndexZipper)
  }

  /** Creates a new IndexedRDD with values from `messages` that shares an index with `this`. */
  def aggregateUsingIndex[V2: ClassTag](
      messages: RDD[(Id, V2)], reduceFunc: (V2, V2) => V2): Self[V2] = {
    this.zipPartitionsWithOther(messages)(new AggregateUsingIndexZipper(reduceFunc))
  }

  // The following functions could have been anonymous, but we name them to work around a Scala
  // compiler bug related to specialization.

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

  private class DeleteZipper extends OtherZipPartitionsFunction[Unit, V] with Serializable {
    def apply(thisIter: Iterator[P[V]], otherIter: Iterator[(Id, Unit)]): Iterator[P[V]] = {
      val thisPart = thisIter.next()
      val deletions = otherIter.map(_._1).toArray
      Iterator(thisPart.delete(deletions))
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
