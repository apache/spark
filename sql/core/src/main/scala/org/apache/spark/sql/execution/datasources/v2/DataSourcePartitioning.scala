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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.sources.v2.reader.partitioning.{ClusteredDistribution, Partitioning}

/**
 * An adapter from public data source partitioning to catalyst internal `Partitioning`.
 */
class DataSourcePartitioning(
    partitioning: Partitioning,
    colNames: AttributeMap[String]) extends physical.Partitioning {

  override val numPartitions: Int = partitioning.numPartitions()

  override def satisfies0(required: physical.Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case d: physical.ClusteredDistribution if isCandidate(d.clustering) =>
          val attrs = d.clustering.map(_.asInstanceOf[Attribute])
          partitioning.satisfy(
            new ClusteredDistribution(attrs.map { a =>
              val name = colNames.get(a)
              assert(name.isDefined, s"Attribute ${a.name} is not found in the data source output")
              name.get
            }.toArray))

        case _ => false
      }
    }
  }

  private def isCandidate(clustering: Seq[Expression]): Boolean = {
    clustering.forall {
      case a: Attribute => colNames.contains(a)
      case _ => false
    }
  }
}
