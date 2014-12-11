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

package org.apache.spark.mllib.feature

import breeze.linalg.SparseVector
import breeze.util.Index
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD



/**
 * This class represents a document in a vector space. Every word in the document is replaced
 * with its serial number.
 *
 * @param tokens Non-zero components correspond to tokens
 *               Non-zero value equals to the number of time the words is included in the document.
 */
class Document(val tokens: SparseVector[Int]) extends Serializable {
  /**
   *
   * @return number of different tokens in the collection
   */
  def alphabetSize = tokens.length
}

/**
 * This class encapsulates string-to-int mapping and can be used for document creation
 * @param alphabet string-to-int mapping
 */
class TokenEnumeration private[mllib](private val alphabet : Index[String]) extends Serializable {

  /**
   *
   * @param rawDocument a sequence of tokens to be transformed
   * @return a Document that contains all the tokens
   *         from rawDocument that are included in the alphabet
   */
  def transform(rawDocument: Seq[String]) : Document = {
    val wordsMap = rawDocument.map(alphabet.apply)
      .filter(_ != -1)
      .foldLeft(Map[Int, Int]().withDefaultValue(0))((map, word) => map + (word -> (1 + map(word))))

    val words = wordsMap.keys.toArray.sorted

    val tokens = new SparseVector[Int](words, words.map(word => wordsMap(word)), alphabet.size)
    new Document(tokens)
  }
}

/**
 * This object enumerates tokens. It assigns an integer to every token.
 * E.g. defines a bijection between set of words and 0 ... (numberOfDifferentTokens - 1)
 */
class TokenEnumerator extends Serializable {
  private var rareTokenThreshold : Int = 2

  /**
   * @param rareTokenThreshold tokens that are encountered in the collection less than
   *                           rareTokenThreshold times are omitted.
   *                           Default value: 2
   */
  def setRareTokenThreshold(rareTokenThreshold : Int) = {
    this.rareTokenThreshold = rareTokenThreshold
    this
  }

  /**
   *
   * @param rawDocuments RDD of tokenized documents (every document is a sequence of tokens
   *                     (Strings) )
   * @return a TokenEnumeration
   */
  def apply(rawDocuments: RDD[Seq[String]]) : TokenEnumeration = {
    val alphabet = Index(rawDocuments.flatMap(x => x)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > rareTokenThreshold)
      .collect
      .map(_._1))

    new TokenEnumeration(alphabet)
  }
}
