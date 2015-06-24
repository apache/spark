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

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.fpm.AssociationRules.Rule

/**
 * :: Experimental ::
 *
 * Generates association rules from a [[FPGrowthModel]] using A Priori's rule-generation procedure
 * described in [[http://www.almaden.ibm.com/cs/quest/papers/vldb94_rj.pdf]].
 */
@Experimental
class AssociationRules private (
    private var minConfidence: Double) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minConfidence = 0.5}.
   */
  def this() = this(0.5)

  /**
   * Sets the minimal confidence (default: `0.5`).
   */
  def setMinConfidence(minConfidence: Double): this.type = {
    this.minConfidence = minConfidence
    this
  }

  /**
   * Computes the association rules with confidence above [[minConfidence]].
   * @param freqItemsets frequent itemsets to compute association rules over.
   * @return a [[Set[Rule[Item]]] containing the assocation rules.
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.map { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) => (antecedent, (consequent, itemset.freq))
        }
      } :+ (items, (new Array[Item](0), itemset.freq))
    }.aggregateByKey(Map[Array[Item], Long]().empty)(
      { case (acc, (consequent, freq)) => acc + (consequent -> freq) },
      { (acc1, acc2) => acc1 ++ acc2 }
    ).flatMap { case (antecedent, consequentToFreq) =>
      consequentToFreq.map { case (consequent, freqUnion) =>
        // TODO: how to get freqAntecedent from consequentToFreq when |Antecedent| > 1 (since
        // |consequent| = 1 and freq(x) + freq(y) != freq(x + y))?
        val freqAntecedent = ???
        Rule(antecedent, consequent, freqUnion / freqAntecedent)
      }
    }
  }
}

object AssociationRules {

  /**
   * :: Experimental ::
   *
   * An association rule between sets of items.
   * @param antecedent hypotheses of the rule
   * @param consequent conclusion of the rule
   * @param confidence the confidence of the rule
   * @tparam Item item type
   */
  @Experimental
  case class Rule[Item: ClassTag](
    antecedent: Array[Item],
    consequent: Array[Item],
    confidence: Double)
    extends Serializable {

    require(!antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet).isEmpty
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })
  }
}
