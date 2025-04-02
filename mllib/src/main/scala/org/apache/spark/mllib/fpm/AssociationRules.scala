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

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ArrayImplicits._

/**
 * Generates association rules from a `RDD[FreqItemset[Item]]`. This method only generates
 * association rules which have a single item as the consequent.
 *
 */
@Since("1.5.0")
class AssociationRules private[fpm] (
    private var minConfidence: Double) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minConfidence = 0.8}.
   */
  @Since("1.5.0")
  def this() = this(0.8)

  /**
   * Sets the minimal confidence (default: `0.8`).
   */
  @Since("1.5.0")
  def setMinConfidence(minConfidence: Double): this.type = {
    require(minConfidence >= 0.0 && minConfidence <= 1.0,
      s"Minimal confidence must be in range [0, 1] but got ${minConfidence}")
    this.minConfidence = minConfidence
    this
  }

  /**
   * Computes the association rules with confidence above `minConfidence`.
   * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
   * @return a `RDD[Rule[Item]]` containing the association rules.
   *
   */
  @Since("1.5.0")
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    run(freqItemsets, Map.empty[Item, Double])
  }

  /**
   * Computes the association rules with confidence above `minConfidence`.
   * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
   * @param itemSupport map containing an item and its support
   * @return a `RDD[Rule[Item]]` containing the association rules. The rules will be able to
   *         compute also the lift metric.
   */
  @Since("2.4.0")
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]],
      itemSupport: scala.collection.Map[Item, Double]): RDD[Rule[Item]] = {
    // For candidate rule X => Y, generate (X, (Y, freq(X union Y)))
    val candidates = freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toImmutableArraySeq, (consequent.toImmutableArraySeq, itemset.freq)))
          case _ => None
        }
      }
    }

    // Join to get (X, ((Y, freq(X union Y)), freq(X))), generate rules, and filter by confidence
    candidates.join(freqItemsets.map(x => (x.items.toImmutableArraySeq, x.freq)))
      .map { case (antecedent, ((consequent, freqUnion), freqAntecedent)) =>
        new Rule(antecedent.toArray,
          consequent.toArray,
          freqUnion.toDouble,
          freqAntecedent.toDouble,
          // the consequent contains always only one element
          itemSupport.get(consequent.head))
      }.filter(_.confidence >= minConfidence)
  }

  /**
   * Java-friendly version of `run`.
   */
  @Since("1.5.0")
  def run[Item](freqItemsets: JavaRDD[FreqItemset[Item]]): JavaRDD[Rule[Item]] = {
    val tag = fakeClassTag[Item]
    run(freqItemsets.rdd)(tag)
  }
}

@Since("1.5.0")
object AssociationRules {

  /**
   * An association rule between sets of items.
   * @param antecedent hypotheses of the rule. Java users should call [[Rule#javaAntecedent]]
   *                   instead.
   * @param consequent conclusion of the rule. Java users should call [[Rule#javaConsequent]]
   *                   instead.
   * @tparam Item item type
   *
   */
  @Since("1.5.0")
  class Rule[Item] private[fpm] (
      @Since("1.5.0") val antecedent: Array[Item],
      @Since("1.5.0") val consequent: Array[Item],
      private[spark] val freqUnion: Double,
      freqAntecedent: Double,
      freqConsequent: Option[Double]) extends Serializable {

    /**
     * Returns the confidence of the rule.
     *
     */
    @Since("1.5.0")
    def confidence: Double = freqUnion / freqAntecedent

    /**
     * Returns the lift of the rule.
     */
    @Since("2.4.0")
    def lift: Option[Double] = freqConsequent.map(fCons => confidence / fCons)

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })

    /**
     * Returns antecedent in a Java List.
     *
     */
    @Since("1.5.0")
    def javaAntecedent: java.util.List[Item] = {
      antecedent.toList.asJava
    }

    /**
     * Returns consequent in a Java List.
     *
     */
    @Since("1.5.0")
    def javaConsequent: java.util.List[Item] = {
      consequent.toList.asJava
    }

    override def toString: String = {
      s"${antecedent.mkString("{", ",", "}")} => " +
        s"${consequent.mkString("{", ",", "}")}: (confidence: $confidence; lift: $lift)"
    }
  }
}
