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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.rdd.RDD

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
   * Computes the association rules with confidence supported above [[minConfidence]].
   * @param freqItemsets frequent itemsets to compute association rules over.
   * @return a [[RDD[Rule[Item]]] containing the assocation rules.
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    freqItemsets.map { itemset =>
      (itemset.items.toSet, itemset.items.map(Set(_)).toSet)
    }.flatMap({ case (itemset, consequents) => generateRules(freqItemsets, itemset, consequents)})
  }

  private def generateRules[Item: ClassTag](
      freqItemsets: RDD[FreqItemset[Item]],
      itemset: Set[Item],
      consequents: Set[Set[Item]]): Set[Rule[Item]] = {
    val k = itemset.size
    val m = consequents.head.size

    val rules = mutable.Set[Rule[Item]]()
    if (k > m+1) {
      val nextConsequents = mutable.Set(aprioriGen[Item](consequents).toSeq: _*)
      for (nextConsequent <- nextConsequents) {
        val proposedRule = Rule[Item](freqItemsets, itemset -- nextConsequent, nextConsequent)
        if (proposedRule.confidence() >= minConfidence) {
          rules += proposedRule
        } else {
          nextConsequents -= nextConsequent
        }
      }
      if (!nextConsequents.isEmpty) rules ++= generateRules[Item](freqItemsets, itemset, nextConsequents.toSet)
    }
    rules.toSet
  }

  private def aprioriGen[Item: ClassTag](itemsets: Set[Set[Item]]): Set[Set[Item]] = {
    val k = itemsets.head.size
    (for {
      p <- itemsets;
      q <- itemsets if p.intersect(q).size == k - 1
    } yield (p ++ q))
      //.filter(_.subsets(k-1).exists(subset => itemsets.contains(subset)))
  }
}

object AssociationRules {

  /**
   * :: Experimental ::
   *
   * An association rule between sets of items.
   * @param freqItemsets dataset used to derive this rule.
   * @param antecedent hypotheses of the rule
   * @param consequent conclusion of the rule
   * @tparam Item item type
   */
  @Experimental
  case class Rule[Item: ClassTag](
    freqItemsets: RDD[FreqItemset[Item]],
    antecedent: Set[Item],
    consequent: Set[Item])
    extends Serializable {

    if (!antecedent.toSet.intersect(consequent.toSet).isEmpty) {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet).isEmpty
      throw new SparkException(s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both.")
    }

    def confidence(): Double = {
      val num = freqItemsets
        .filter(_.items.toSet == (antecedent ++ consequent))
        .first()
        .freq
      val denom = freqItemsets
        .filter(_.items.toSet == antecedent)
        .first()
        .freq

      num.toDouble / denom.toDouble
    }
  }
}
