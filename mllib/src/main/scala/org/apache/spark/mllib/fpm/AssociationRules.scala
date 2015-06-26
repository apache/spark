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

import java.lang.{Iterable => JavaIterable}

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.fpm.AssociationRules.Rule
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
   * @param model frequent itemset model obtained from [[FPGrowth]]
   * @return a [[Set[Rule[Item]]] containing the assocation rules.
   */
  def run[Item: ClassTag](model: FPGrowthModel[Item]): RDD[Rule[Item]] = {
    val freqItemsets = model.freqItemsets
    val numTransactions = model.numTransactions

    freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.map { item =>
        // Key using List[Item] because cannot use Array[Item] for map-side combining
        items.partition(_ == item) match {
          case (consequent, antecedent) => (antecedent.toList, (consequent.toList, itemset.freq))
        }
      } :+ (items.toList, (Nil, itemset.freq))
    }.aggregateByKey(Map[List[Item], Long]().empty)(
      { case (acc, (consequent, freq)) => acc + (consequent -> freq) },
      { (acc1, acc2) => acc1 ++ acc2 }
    ).flatMap { case (antecedent, consequentToFreq) =>
      consequentToFreq.flatMap { case (consequent, freqUnion) =>
        val freqAntecedent = if (antecedent == Nil) numTransactions else consequentToFreq(Nil)
        val confidence = freqUnion.toDouble / freqAntecedent.toDouble
        if (!consequent.isEmpty && confidence >= minConfidence) {
          Some(Rule[Item](antecedent.toArray, consequent.toArray, confidence))
        } else {
          None
        }
      }
    }
  }

  def runJava[Item](model: FPGrowthModel[Item]): JavaRDD[Rule[Item]] = {
    implicit val tag = fakeClassTag[Item]
    run(model)
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

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })
  }
}
