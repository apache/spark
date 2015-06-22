package org.apache.spark.mllib.fpm

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * :: Experimental ::
 *
 * Generates association rules from a [[FPGrowthModel]] using A Priori's rule-generation procedure
 * described in [[http://www.almaden.ibm.com/cs/quest/papers/vldb94_rj.pdf]].
 */
@Experimental
class AssociationRules[Item: ClassTag] private (
    private var minConfidence: Double) extends Logging with Serializable {

  def run(model: FPGrowthModel[Item]): RDD[Set[Rule[Item]]] = {
    model.freqItemsets.flatMap { itemset =>
      itemset.items.map(item => (itemset.items.toSet, Set(Set(item))))
    }.map({ case (itemset, consequents) => generateRules(model, itemset, consequents)})
  }

  private def aprioriGen(itemsets: Set[Set[Item]]): Set[Set[Item]] = {
    val k = itemsets.head.size
    (for {
      p <- itemsets;
      q <- itemsets if p.intersect(q) == k - 1;
    } yield p ++ q)
      .filter(_.subsets(k-1).exists(subset => itemsets.contains(subset)))
  }

  private def generateRules(
      model: FPGrowthModel[Item],
      itemset: Set[Item],
      consequents: Set[Set[Item]]): Set[Rule[Item]] = {
    val k = itemset.size
    val m = consequents.size

    val rules = mutable.Set[Rule[Item]]()
    if (k > m+1) {
      val nextConsequents = mutable.Set(aprioriGen(consequents).toSeq: _*)
      for (nextConsequent <- nextConsequents) {
        val proposedRule = Rule[Item](model, itemset -- nextConsequent, nextConsequent)
        if (proposedRule.confidence() >= minConfidence) rules.add(proposedRule)
        else nextConsequents -= nextConsequent
      }
      rules ++= generateRules(model, itemset, nextConsequents.toSet)
    }
    rules.toSet
  }
}

object AssociationRules {
  /**
   * :: Experimental ::
   *
   * An association rule between sets of items.
   * @param model [[FPGrowthModel]] used to generate this rule.
   * @param antecedent hypotheses of the rule
   * @param consequent conclusion of the rule
   * @tparam Item item type
   */
  @Experimental
  case class Rule[Item: ClassTag](
    model: FPGrowthModel[Item],
    antecedent: Set[Item],
    consequent: Set[Item])
    extends Serializable {

    if (!antecedent.toSet.intersect(consequent.toSet).isEmpty) {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet).isEmpty
      throw new SparkException(s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both.")
    }

    def confidence(): Double = {
      val num = model.freqItemsets
        .filter(_.items.toSet == (antecedent ++ consequent).toSet)
        .first()
        .freq
      val denom = model.freqItemsets
        .filter(_.items.toSet == (antecedent).toSet)
        .first()
        .freq

      num.toDouble / denom.toDouble
    }
  }
}
