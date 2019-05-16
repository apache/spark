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

package org.apache.spark.sql.catalyst.optimizer.ga

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression}
import org.apache.spark.sql.catalyst.optimizer.{JoinGraphInfo, JoinReorderDP}
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.JoinPlan
import org.apache.spark.sql.internal.SQLConf

case class Chromosome(
    conf: SQLConf,
    basicPlans: Seq[JoinPlan],
    conditions: Set[Expression],
    topOutputSet: AttributeSet) {

  lazy val fitness: Double = evalFitness(integratedPlan)

  lazy val integratedPlan: Option[JoinPlan] = makePlan

  private def makePlan: Option[JoinPlan] = {
    val semiFinished = mutable.Buffer[JoinPlan]()
    basicPlans.foreach(mergeSemi(semiFinished, _))
    if (semiFinished.head.itemIds.size == basicPlans.size) {
      Some(semiFinished.head)
    } else {
      None
    }
  }

  private def mergeSemi(semiFinished: mutable.Buffer[JoinPlan], right: JoinPlan): Unit = {
    val filters = None: Option[JoinGraphInfo]
    for (left <- semiFinished) {
      JoinReorderDP.buildJoin(left, right, conf, conditions, topOutputSet, filters) match {
        case Some(joined) =>
          semiFinished.remove(semiFinished.indexOf(left))
          mergeSemi(semiFinished, joined)
        case _ =>
          None
      }
    }

    if (semiFinished.isEmpty || right.itemIds.size == 1) {
      semiFinished.append(right)
      return
    }

    insertPlan(semiFinished, right)
  }

  private def insertPlan(semiFinished: mutable.Buffer[JoinPlan], plan: JoinPlan): Unit = {
    var criticalSize = if (semiFinished.head.itemIds.size > plan.itemIds.size) {
      semiFinished.head.itemIds.size
    } else {
      plan.itemIds.size
    }
    var criticalIndex = 0
    var break: Boolean = false
    for (p <- semiFinished if !break) {
      if (plan.itemIds.size > p.itemIds.size && plan.itemIds.size <= criticalSize) {
        break = true
      } else {
        criticalIndex += 1
        criticalSize = p.itemIds.size
      }
    }

    semiFinished.insert(criticalIndex, plan)
  }

  private def evalFitness(plan: Option[JoinPlan]): Double = {
    plan match {
      case Some(joinPlan) =>
        // We use the negative cost as fitness.
        - joinPlan.planCost.card.toDouble * conf.joinReorderCardWeight -
            joinPlan.planCost.size.toDouble * (1 - conf.joinReorderCardWeight)
      case _ =>
        - Double.MaxValue
    }
  }
}

object Population {
  def apply(
      conf: SQLConf,
      itemsMap: Map [Int, JoinPlan],
      conditions: Set[Expression],
      topOutputSet: AttributeSet) : Population = {

    var chromos: Seq[Chromosome] = Seq()
    var i = 0
    val popSize = determinePopSize(conf, itemsMap.size)
    while(i < popSize) {
      chromos = chromos :+ Chromosome(conf, shuffle(itemsMap), conditions, topOutputSet)
      i += 1
    }

    new Population(conf, chromos)
  }

  private def determinePopSize(conf: SQLConf, numRelations: Int): Int = {
    val relaxFactor = conf.joinReorderGARelaxFactor
    // The default population size:
    // # of relations | pop size (RF=3) | pop size (RF=3.5)| pop size  (RF=4)
    //  < 13          |   DP based      |   DP based       |   DP based
    //    13          |   20            |   16 (13<16)     |   16
    //    14          |   25            |   16             |   16
    //    15          |   32            |   19             |   16
    //    16          |   40            |   23             |   16
    //    17          |   50            |   28             |   19
    //    18          |   64            |   35             |   22
    //    19          |   80            |   43             |   26
    //    20          |   101           |   52             |   32
    //    21          |   128           |   64             |   38
    //    22          |   128           |   78             |   45
    //    23          |   128           |   95             |   53
    //    24          |   128           |   115            |   64
    //    25          |   128           |   128            |   76
    //    26          |   128           |   128            |   90
    //    27          |   128           |   128            |   90
    //    28          |   128           |   128            |   128
    //  > 28          |   128           |   128            |   128
    val size = math.pow(2.0, numRelations / relaxFactor)
    val max = conf.joinReorderGAMaxPoPSize
    val min = conf.joinReorderGAMinPoPSize

    if (size > max) {
      return max
    }
    if (size < min) {
      return min
    }
    math.ceil(size).toInt
  }

  private def shuffle(itemsMap: Map[Int, JoinPlan]) : Seq[JoinPlan] = {
    val shuffled = util.Random.shuffle(itemsMap.keySet.toList)
    val sb = Seq.newBuilder[JoinPlan]
    shuffled.foreach(index => sb += itemsMap(index))
    sb.result()
  }
}

class Population(conf: SQLConf, var chromos: Seq[Chromosome]) extends Logging {
  private def sort(): Unit = {
    chromos = chromos.sortWith((left, right) => left.fitness > right.fitness)
  }

  def evolve: Population = {
    // Sort chromos in the population first.
    sort()
    // Begin iteration.
    var i = 0
    val generations = chromos.size
    val crossover = EdgeRecombination
    while (i < generations) {
      val baba = GAUtils.select(conf, chromos, None)
      val mama = GAUtils.select(conf, chromos, Some(baba))
      val kid = crossover.newChromo(baba, mama)
      chromos = putToPop(kid)
      logInfo(s"Iteration $i, fitness for kid: ${kid.fitness}," +
          s" and Fitness for plans: ${chromos.map(c => c.fitness)}")
      i += 1
    }
    this
  }

  private def putToPop(kid: Chromosome): Seq[Chromosome] = {
    val tmp = mutable.Buffer[Chromosome]()
    var added = false
    for (elem <- chromos) {
      if (kid.fitness >= elem.fitness && !added) {
        tmp.append(kid)
        added = true
      }
      tmp.append(elem)
    }
    // Remove the last elem whose fitness is lowest if we have inserted the kid.
    if (added) {
      tmp.remove(tmp.size - 1)
    }
    tmp
  }
}

object GAUtils {
  def select(conf: SQLConf, chromos: Seq[Chromosome], ex: Option[Chromosome]): Chromosome = {
    ex match {
      case Some(e) =>
        val exIndex = chromos.indexOf(e)
        val index = {
          var candidate: Int = 0
          var break: Boolean = false
          while (!break) {
            candidate = rand(chromos.size, conf.joinReorderGASelectionBias)
            if (candidate != exIndex) {
              break = true
            }
          }
          candidate
        }
        chromos(index)
      case None =>
        chromos(rand(chromos.size, conf.joinReorderGASelectionBias))
    }
  }

  private def rand(cap: Int, bias: Double) : Int = {
    var index: Double = 0
    do {
      index = cap * (bias - math.sqrt((bias * bias) -
          4.0 * (bias - 1.0) * util.Random.nextDouble)) / 2.0 / (bias - 1.0)
    } while (index < 0.0 || index >= cap)

    index.toInt
  }
}
