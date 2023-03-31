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

package org.apache.spark.sql.execution.exchange

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnionExec, UnionZipExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Pushes parent's required distribution to [[UnionExec]] so that we can
 * remove unnecessary shuffle exchange if any child output partitioning matches
 * 1) the operator is UnionExec
 * 2) the operator's output partitioning can not satisfy parent distribution
 * 3) the operator's parent requires hashing or clustering distribution
 * 4) the data types of operator's children output attributes are the same
 */
object UnionDistributionPushdown extends Rule[SparkPlan] {

  def allChildrenSameDataType(unionExec: UnionExec): Boolean = {
    (Seq(unionExec) ++ unionExec.children).map(_.output).transpose.map{ attrs =>
      val firstAttr = attrs.head
      attrs.forall(_.dataType == firstAttr.dataType)
    }.forall(_ == true)
  }

  def createNewChildrenDistribution(unionExec: UnionExec,
                                    unionRequired: Distribution): Seq[Distribution] = {
    unionExec.children.map{ unionChild =>
      // create output attributes map (union output exprId -> child output)
      val attributes = unionExec.output.map(_.exprId).zip(unionChild.output).toMap
      unionRequired match {
        case distribution: ClusteredDistribution =>
          val newExpressions = distribution.clustering.map(exp => exp.transformUp {
            case attr: Attribute if attributes.contains(attr.exprId) => attributes(attr.exprId)
          })
          ClusteredDistribution(newExpressions, distribution.requireAllClusterKeys,
            distribution.requiredNumPartitions)

        case distribution => distribution
      }
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan if conf.getConf(SQLConf.UNION_REQUIRED_DISTRIBUTION_PUSHDOWN) &&
      operator.children.exists(_.isInstanceOf[UnionExec]) =>
      val newChildren = operator.children.zipWithIndex.map {
        case (unionExec: UnionExec, childIndex: Int) if !unionExec.outputPartitioning.
          satisfies(operator.requiredChildDistribution(childIndex)) =>
          val unionRequired = operator.requiredChildDistribution(childIndex)
          unionRequired match {
            case _: ClusteredDistribution if allChildrenSameDataType(unionExec) =>
              val requiredChildDistribution: Seq[Distribution] =
                createNewChildrenDistribution(unionExec, unionRequired)
              val numPartitions = unionRequired.requiredNumPartitions
                .getOrElse(conf.numShufflePartitions)
              val outputPartitioning = unionRequired.createPartitioning(numPartitions)
              UnionZipExec(unionExec.children, requiredChildDistribution,
                outputPartitioning)

            case _: Distribution => unionExec
          }

        case (child, _) => child
      }
      operator.withNewChildren(newChildren)
  }
}

/**
 * Class representing partitions of UnionZipRDD, which maintains the list of
 * corresponding partitions of parent RDDs.
 */
private[spark]
class UnionZipRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    override val index: Int
  ) extends Partition {
  var parents: Array[Partition] = rdds.map(_.partitions(index)).toArray

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
}

/**
 * Class representing an RDD that can take multiple RDDs with same partition size and
 * unify them into a single RDD while preserving the partition. So m RDDs with p partitions
 * will be unified to a single RDD with p partitions, which the ith partition consists of the
 * ith partition in each parent RDD.
 */
private[spark]
class UnionZipRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.nonEmpty)
  require(rdds.map(_.partitions.length).toSet.size == 1,
    "Parent RDDs have different partition size: " + rdds.map(_.partitions.length))

  override def getPartitions: Array[Partition] = {
    val numPartitions = rdds.head.partitions.length
    (0 until numPartitions).map { index =>
      new UnionZipRDDPartition(rdds, index)
    }.toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[UnionZipRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
