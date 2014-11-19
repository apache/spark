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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkContext, Logging, Partition, TaskContext}
import org.apache.spark.{Dependency, NarrowDependency, OneToOneDependency}


private[spark]
class CascadeDep[T: ClassTag](rdd: RDD[T], pid: Int) extends NarrowDependency[T](rdd) {
  // each cascaded dependency is one particular partition in the given rdd
  override def getParents(unused: Int) = List(pid) 
}


private[spark]
class CascadePartition extends Partition {
  // each CascadeRDD has one partition
  override def index = 0
}


private[spark]
class CascadeRDD[T: ClassTag, U: ClassTag]
  (rdd: RDD[T], pid: Int, cascade: Option[RDD[U]], 
   f: => ((Iterator[T], Option[Iterator[U]]) => Iterator[U]))
  extends RDD[U](rdd.context,
                 cascade match {
                   case None => List(new CascadeDep(rdd, pid))
                   case Some(crdd) => List(new CascadeDep(rdd, pid), new CascadeDep(crdd, 0))
                 }) {

  val rddPartition = rdd.partitions(pid)

  override def getPartitions: Array[Partition] = Array(new CascadePartition)

  override def compute(unused: Partition, ctx: TaskContext): Iterator[U] = {
    f(rdd.iterator(rddPartition, ctx), cascade.map(_.iterator(new CascadePartition, ctx)))
  }
}


class CascadeRDDFunctions[T: ClassTag](self: RDD[T]) extends Logging with Serializable {

  /*
   * Applies a "cascading" function to the input RDD, such that each output partition is
   * a function of the corresponding input partition and the previous output partition
   */
  def cascadePartitions[U: ClassTag]
    (f: => ((Iterator[T], Option[Iterator[U]]) => Iterator[U])): RDD[U] = {
    if (self.partitions.length <= 0) return self.context.emptyRDD[U]

    val fclean = self.context.clean(f)

    val cascade = ArrayBuffer[RDD[U]](new CascadeRDD(self, 0, None, fclean))

    for (j <- 1 until self.partitions.length) {
      val prev = cascade.last
      cascade += new CascadeRDD(self, j, Some(prev), fclean)
    }

    new UnionRDD(self.context, cascade)
  }

}
