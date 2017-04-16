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

package org.apache.spark.mllib.preprocessing

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

/**
 * A utility for encoding categorical variables as numeric variables. The resulting vectors
 * contain a component for each value that the variable can take. The component corresponding
 * to the value that the variable takes is set to 1 and the components corresponding to all other
 * categories are set to 0 - [[http://en.wikipedia.org/wiki/One-hot]].
 *
 * The utility handles input vectors with mixed categorical and numeric variables by accepting a
 * list of feature indices that are categorical and only transforming those.
 *
 * The utility can transform vectors such as:
 * {{{
 *   (1.7, "apple", 2.0)
 *   (4.9, "banana", 5.6)
 *   (8.0, "pear", 6.0)
 * }}}
 *
 * Into:
 * {{{
 *   (1.7, 1, 0, 0, 2.0)
 *   (4.9, 0, 1, 0, 5.6)
 *   (8.0, 0, 0, 1, 6.0)
 * }}}
 *
 * An example usage is:
 *
 * {{{
 *  val categoricalFields = Array(0, 7, 21)
 *  val categories = OneHotEncoder.categories(rdd, categoricalFields)
 *  val encoded = OneHotEncoder.encode(rdd, categories)
 * }}}
 */
object OneHotEncoder {

  /**
   * Given a dataset and the set of columns which are categorical variables, returns a structure
   * that, for each field, describes the values that are present for in the dataset. The structure
   * is meant to be used as input to the encode method.
   */
  def categories[T](rdd: RDD[Array[T]], catFields: Seq[Int]): Array[(Int, Map[T, Int])] = {
    val categoriesArr = new Array[mutable.Set[T]](catFields.length)
    for (i <- 0 until catFields.length) {
      categoriesArr(i) = new mutable.HashSet[T]()
    }

    val categories = rdd.aggregate(categoriesArr)(mergeElement(catFields), mergeSets)

    val catMaps = new Array[(Int, Map[T, Int])](catFields.length)
    for (i <- 0 until catFields.length) {
      catMaps(i) = (catFields(i), categories(i).zipWithIndex.toMap)
    }

    catMaps
  }

  private def mergeElement[T](catFields: Seq[Int])(a: Array[mutable.Set[T]], b: Array[T]):
      Array[mutable.Set[T]] = {
    var i = 0
    while (i < catFields.length) {
      a(i) += b(catFields(i))
      i += 1
    }
    a
  }

  private def mergeSets[T](a: Array[mutable.Set[T]], b: Array[mutable.Set[T]]):
      Array[mutable.Set[T]] = {
    var i = 0
    while (i < a.length) {
      a(i) ++= b(i)
      i += 1
    }
    a
  }

  /**
   * OneHot encodes the given RDD.
   */
  def encode[T:ClassTag](rdd: RDD[Array[T]], featureCategories: Array[(Int, Map[T, Int])]):
      RDD[Array[T]] = {
    var outArrLen = rdd.first().length
    for (catMap <- featureCategories) {
      outArrLen += (catMap._2.size - 1)
    }
    rdd.map(encodeVec[T](_, featureCategories, outArrLen))
  }

  private def encodeVec[T:ClassTag](vec: Array[T], featureCategories: Array[(Int, Map[T, Int])],
      outArrLen: Int): Array[T] = {
    var outArrIndex = 0
    val outVec = new Array[T](outArrLen)
    var i = 0
    var featureCatIndex = 0
    val zero = 0.asInstanceOf[T]
    val one = 1.asInstanceOf[T]
    while (i < vec.length) {
      if (featureCatIndex < featureCategories.length &&
            featureCategories(featureCatIndex)._1 == i) {
        var j = outArrIndex
        val catVals = featureCategories(featureCatIndex)._2
        while (j < outArrIndex + catVals.size) {
          outVec(j) = zero
          j += 1
        }
        outVec(outArrIndex + catVals.getOrElse(vec(i), -1)) = one
        outArrIndex += catVals.size
        featureCatIndex += 1
      } else {
        outVec(outArrIndex) = vec(i)
        outArrIndex += 1
      }

      i += 1
    }
    outVec
  }

}
