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

import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashSet
import scala.collection.mutable.Set

/**
 * A utility for encoding categorical variables as numeric variables. The resulting vectors
 * contain a component for each value that the variable can take. The component corresponding
 * to the value that the variable takes is set to 1 and the components corresponding to all other
 * categories are set to 0.
 *
 * The utility handles input vectors with mixed categorical and numeric variables by accepting a
 * list of feature indices that are categorical and only transforming those.
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
   * is meant to be used as input to encode.
   */
  def categories(rdd: RDD[Array[Any]], categoricalFields: Seq[Int]): Array[Map[Any, Int]] = {
    val categories = rdd.map(categoricals(_, categoricalFields)).reduce(uniqueCats)

    val catMaps = new Array[Map[Any, Int]](rdd.first().length)
    for (i <- 0 until categoricalFields.length) {
      catMaps(categoricalFields(i)) = categories(i).zipWithIndex.toMap
    }

    catMaps
  }

  /**
   * Accepts a vector and set of feature indices that are categorical variables.  Outputs an array
   * whose size is the number of categorical fields and each element is a Set of size one
   * containing a categorical value from the input vec
   */
  private def categoricals(tokens: Array[Any], catFields: Seq[Int]): Array[Set[Any]] = {
    val categoriesArr = new Array[Set[Any]](catFields.length)
    for (i <- 0 until catFields.length) {
      categoriesArr(i) = new HashSet[Any]()
      categoriesArr(i) += tokens(catFields(i))
    }
    categoriesArr
  }

  private def uniqueCats(a: Array[Set[Any]], b: Array[Set[Any]]): Array[Set[Any]] = {
    for (i <- 0 until a.length) {
      a(i) ++= b(i)
    }
    a
  }

  /**
   * OneHot encodes the given RDD.
   */
  def encode(rdd: RDD[Array[Any]], featureCategories: Array[Map[Any, Int]]):
      RDD[Array[Any]] = {
    var outArrLen = 0
    for (catMap <- featureCategories) {
      outArrLen += (if (catMap == null) 1 else catMap.size)
    }
    rdd.map(encodeVec(_, featureCategories, outArrLen))
  }

  private def encodeVec(vec: Array[Any], featureCategories: Array[Map[Any, Int]],
      outArrLen: Int): Array[Any] = {
    var outArrIndex = 0
    val outVec = new Array[Any](outArrLen)
    for (i <- 0 until vec.length) {
      if (featureCategories(i) != null) {
        for (j <- outArrIndex until outArrIndex + featureCategories(i).size) {
          outVec(j) = 0
        }
        outVec(outArrIndex + featureCategories(i).getOrElse(vec(i), -1)) = 1
        outArrIndex += featureCategories(i).size
      } else {
        outVec(outArrIndex) = vec(i)
        outArrIndex += 1
      }
    }
    outVec
  }

}
