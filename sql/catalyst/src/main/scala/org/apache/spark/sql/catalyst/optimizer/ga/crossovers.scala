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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.JoinPlan

@DeveloperApi
trait Crossover {
  def newChromo(baba: Chromosome, mama: Chromosome) : Chromosome
}

case class EdgeTable(table: Map[JoinPlan, Seq[JoinPlan]])

/**
 * This class implements the Genetic Edge Recombination algorithm.
 * For more information about the Genetic Edge Recombination,
 * see "Scheduling Problems and Traveling Salesmen: The Genetic Edge
 * Recombination Operator" by L. Darrell Whitley etc.
 * https://dl.acm.org/citation.cfm?id=657238
 */
object EdgeRecombination extends Crossover {

  def genEdgeTable(baba: Chromosome, mama: Chromosome) : EdgeTable = {
    val tabBaba = baba.basicPlans.map(g => g -> findNeighbours(baba.basicPlans, g)).toMap
    val tabMama = mama.basicPlans.map(g => g -> findNeighbours(mama.basicPlans, g)).toMap
    EdgeTable(tabBaba.map(entry => entry._1 -> (entry._2 ++ tabMama(entry._1))))
  }

  def findNeighbours(genes: Seq[JoinPlan], g: JoinPlan) : Seq[JoinPlan] = {
    val genesIndexed = genes.toIndexedSeq
    val index = genesIndexed.indexOf(g)
    val length = genes.size
    if (index > 0 && index < length - 1) {
      Seq(genesIndexed(index - 1), genesIndexed(index + 1))
    } else if (index == 0) {
      Seq(genesIndexed(1), genesIndexed(length - 1))
    } else if (index == length - 1) {
      Seq(genesIndexed(0), genesIndexed(length - 2))
    } else {
      Seq()
    }
  }

  override def newChromo(baba: Chromosome, mama: Chromosome): Chromosome = {
    var newGenes: Seq[JoinPlan] = Seq()
    // 1. Generate the edge table.
    var table = genEdgeTable(baba, mama).table
    // 2. Choose a start point randomly from the heads of baba/mama.
    var current = if (util.Random.nextInt(2) == 0) baba.basicPlans.head else mama.basicPlans.head
    newGenes :+= current

    var stop = false
    while (!stop) {
      // 3. Filter out the chosen point from the edge table.
      table = table.map(
        entry => entry._1 -> entry._2.filter(g => if (g == current) false else true)
      )
      // 4. Choose next point among its neighbours. The criterion for choosing which point
      // is that the one who has fewest neighbours. If two or more points has the same num
      // of neighbours, choose one randomly. If there's no neighbours available for this
      // point but there're still other remaining points, choose one from them randomly.
      val tobeVisited = table(current)
      val neighboursTable = tobeVisited.map(g => g -> table(g)).sortBy(-_._2.size).toMap
      val filteredTable = table.filter(_._2.nonEmpty)
      if (neighboursTable.nonEmpty) {
        val numBase = neighboursTable.head._2.size
        var numCand = 0
        neighboursTable.foreach(entry => if (entry._2.size == numBase) numCand += 1)
        current = neighboursTable.toIndexedSeq(util.Random.nextInt(numCand))._1
        newGenes :+= current
      } else if (filteredTable.nonEmpty) {
        current = filteredTable.toIndexedSeq(util.Random.nextInt(filteredTable.size))._2.head
        newGenes :+= current
      } else {
        stop = true
      }
    }

    Chromosome(baba.conf, newGenes, baba.conditions, baba.topOutputSet)
  }
}
