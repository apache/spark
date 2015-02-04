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

import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, Logging, Partition, TaskContext, 
                         Dependency, NarrowDependency}


private [spark]
class FanOutDep[T: ClassTag](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // Assuming child RDD type having only one partition
  override def getParents(pid: Int) = (0 until rdd.partitions.length)
}


private [spark]
class PromisePartition extends Partition {
  // A PromiseRDD has exactly one partition, by construction:
  override def index = 0
}


/**
 * A way to represent the concept of a promised expression as an RDD, so that it
 * can operate naturally inside the lazy-transform formalism
 */
private [spark]
class PromiseRDD[V: ClassTag](expr: => (TaskContext => V),
                              context: SparkContext, deps: Seq[Dependency[_]])
  extends RDD[V](context, deps) {

  // This RDD has exactly one partition by definition, since it will contain
  // a single row holding the 'promised' result of evaluating 'expr' 
  override def getPartitions = Array(new PromisePartition)

  // compute evaluates 'expr', yielding an iterator over a sequence of length 1:
  override def compute(p: Partition, ctx: TaskContext) = List(expr(ctx)).iterator
}


/**
 * A partition that augments a standard RDD partition with a list of PromiseRDD arguments,
 * so that they are available at partition compute time
 */
private [spark]
class PromiseArgPartition(p: Partition, argv: Seq[PromiseRDD[_]]) extends Partition {
  override def index = p.index

  /**
   * obtain the underlying partition
   */
  def partition: Partition = p

  /**
   * Compute the nth PromiseRDD argument's expression and return its value
   * The return type V must be provided explicitly, and be compatible with the
   * actual type of the PromiseRDD.
   */
  def arg[V](n: Int, ctx: TaskContext): V = 
    argv(n).iterator(new PromisePartition, ctx).next.asInstanceOf[V]
}


/**
 * Extra functions available on RDDs for providing the RDD analogs of Scala drop,
 * dropRight and dropWhile, which return an RDD as a result
 */
class PromiseRDDFunctions[T : ClassTag](self: RDD[T]) extends Logging with Serializable {

  /**
   * Return a PromiseRDD by applying function 'f' to the partitions of this RDD
   */
  def promiseFromPartitions[V: ClassTag](f: Seq[Iterator[T]] => V): PromiseRDD[V] = {
    val rdd = self
    val plist = rdd.partitions
    val expr = self.context.clean((ctx: TaskContext) => f(plist.map(s => rdd.iterator(s, ctx))))
    new PromiseRDD[V](expr, rdd.context, List(new FanOutDep(rdd))) 
  }

  /**
   * Return a PromiseRDD by applying function 'f' to a partition array.
   * This can allow improved efficiency over promiseFromPartitions(), as it does not force
   * call to iterator() method over entire partition list, if 'f' does not require it
   */
  private [spark]
  def promiseFromPartitionArray[V: ClassTag](f: (Array[Partition], 
                                             RDD[T], TaskContext) => V): PromiseRDD[V] = {
    val rdd = self
    val plist = rdd.partitions
    val expr = self.context.clean((ctx: TaskContext) => f(plist, rdd, ctx))
    new PromiseRDD[V](expr, rdd.context, List(new FanOutDep(rdd))) 
  }

}
