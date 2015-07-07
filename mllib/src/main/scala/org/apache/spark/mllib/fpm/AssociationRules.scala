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
package org.apache.spark.mllib.fpm

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
 * Generates association rules from a [[RDD[FreqItemset[Item]]]. This method only generates
 * association rules which have a single item as the consequent.
 */
@Experimental
class AssociationRules private (
    private var minConfidence: Double) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minConfidence = 0.8}.
   */
  def this() = this(0.8)

  /**
   * Sets the minimal confidence (default: `0.8`).
   */
  def setMinConfidence(minConfidence: Double): this.type = {
    this.minConfidence = minConfidence
    this
  }

  /**
   * Computes the association rules with confidence above [[minConfidence]].
   * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
   * @return a [[Set[Rule[Item]]] containing the assocation rules.
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          // Itemsets and items in itemsets are unique, so every (antecedent, consequent) is unique
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
          case _ => None
        }
      } :+ (items.toSeq, (Nil, itemset.freq))
    }.aggregateByKey(Map[Seq[Item], Long]().empty)(
      // Since every (antecedent, consequent) is unique, there are no collisions in the Map
      seqOp = { case (acc, (consequent, freq)) => acc + (consequent -> freq) },
      combOp = _ ++ _
    ).flatMap { case (antecedent, consequentToFreq) =>
      consequentToFreq.flatMap { case (consequent, freqUnion) =>
        val freqAntecedent = consequentToFreq(Nil)
        val confidence = 1.0 * freqUnion / freqAntecedent
        if (!consequent.isEmpty && confidence >= minConfidence) {
          Some(new Rule[Item](antecedent.toArray, consequent.toArray, freqUnion, freqAntecedent))
        } else {
          None
        }
      }
    }
  }

  def run[Item](freqItemsets: JavaRDD[FreqItemset[Item]]): JavaRDD[Rule[Item]] = {
    val tag = fakeClassTag[Item]
    run(freqItemsets.rdd)(tag)
  }
}

object AssociationRules {

  /**
   * :: Experimental ::
   *
   * An association rule between sets of items.
   * @param antecedent hypotheses of the rule
   * @param consequent conclusion of the rule
   * @param freqUnion the frequency (num. occurrences) of the union of the antecedent and consequent
   *                  as subsets of transactions in the transaction data
   * @param freqAntecedent the frequency of the antecedent in the transaction data
   * @tparam Item item type
   */
  @Experimental
  class Rule[Item] private[mllib] (
      antecedent: Array[Item],
      consequent: Array[Item],
      freqUnion: Double,
      freqAntecedent: Double) extends Serializable {

    def confidence: Double = 1.0 * freqUnion / freqAntecedent

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })
  }
}
