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

package org.apache.spark.graphx.api.java

import java.lang.{Integer => JInt, Long => JLong, Boolean => JBoolean}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaUtils
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2,
  Function3 => JFunction3, Function4 => JFunction4, _}
import org.apache.spark.graphx._

class JavaEdgeRDD[ED, VD](override val rdd: EdgeRDD[ED, VD])(
    implicit val edTag: ClassTag[ED], val vdTag: ClassTag[VD])
  extends JavaRDD[Edge[ED]](rdd) {

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  def mapValues[ED2](f: JFunction[Edge[ED], ED2]): JavaEdgeRDD[ED2, VD] = {
    implicit val ed2Tag: ClassTag[ED2] = fakeClassTag
    rdd.mapValues(f)
  }

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  def reverse(): JavaEdgeRDD[ED, VD] = rdd.reverse

  /** Removes all edges but those matching `epred` and where both vertices match `vpred`. */
  def filter(
      epred: JFunction[EdgeTriplet[VD, ED], JBoolean],
      vpred: JFunction2[JLong, VD, JBoolean]): JavaEdgeRDD[ED, VD] =
    rdd.filter(et => epred.call(et), (id, attr) => vpred.call(id, attr))

  /**
   * Inner joins this EdgeRDD with another EdgeRDD, assuming both are partitioned using the same
   * [[PartitionStrategy]].
   *
   * @param other the EdgeRDD to join with
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
   *         with values supplied by `f`
   */
  def innerJoin[ED2, ED3](
      other: JavaEdgeRDD[ED2, _],
      f: JFunction4[JLong, JLong, ED, ED2, ED3]): JavaEdgeRDD[ED3, VD] = {
    implicit val ed2Tag: ClassTag[ED2] = fakeClassTag
    implicit val ed3Tag: ClassTag[ED3] = fakeClassTag
    rdd.innerJoin(other) { (src, dst, a, b) => f(src, dst, a, b) }
  }
}

object JavaEdgeRDD {

  def fromEdges[ED, VD](edges: JavaRDD[Edge[ED]]): JavaEdgeRDD[ED, VD] = {
    implicit val edTag: ClassTag[ED] = fakeClassTag
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    fromEdgeRDD(EdgeRDD.fromEdges(edges))
  }

  implicit def fromEdgeRDD[ED: ClassTag, VD: ClassTag](rdd: EdgeRDD[ED, VD]): JavaEdgeRDD[ED, VD] =
    new JavaEdgeRDD[ED, VD](rdd)

  implicit def toEdgeRDD[ED, VD](rdd: JavaEdgeRDD[ED, VD]): EdgeRDD[ED, VD] =
    rdd.rdd
}
