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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, SortOrder}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.  Distribution can be used to refer to two distinct physical
 * properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
 */
sealed trait Distribution

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnspecifiedDistribution extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located. Based on the context, this
 * can mean such tuples are either co-located in the same partition or they will be contiguous
 * within a single partition.
 * There is also another constraint, the `clustering` value contains null will be considered
 * as a valid value if `nullKeysSensitive` == true.
 *
 * For examples:
 * JOIN KEYS: values contains null will be considered as invalid values, which means
 *          the tuples could be in different partition.
 * GROUP BY KEYS: values contains null will be considered as the valid value, which means
 *          the tuples should be in the same partition.
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    nullKeysSensitive: Boolean,
    sortKeys: Seq[SortOrder] = Nil) extends Distribution

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]].  This is a strictly stronger guarantee than
 * [[ClusteredDistribution]] as an ordering will ensure that tuples that share the same value for
 * the ordering expressions are contiguous and will never be split across partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of a OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  // TODO: This is not really valid...
  def clustering: Set[Expression] = ordering.map(_.child).toSet
}

/**
 * A gap that represents what need to be done for the satisfying the its parent operator in
 * data distribution.
 */
private[sql] sealed trait Gap

/**
 * Needn't do anything for the data distribution.
 */
private[sql] case object NoGap extends Gap

/**
 * Need to sort the data within the current partition.
 * @param sortKeys the sorting keys
 */
private[sql] case class SortKeyWithinPartition(sortKeys: Seq[SortOrder]) extends Gap

/**
 * Need a global sorting for the distribution according to the specified sorting keys.
 * @param ordering the sorting keys
 */
private[sql] case class GlobalOrdering(ordering: Seq[SortOrder]) extends Gap

/**
 * Repartition the data according to the new clustering expression, and it's possible that
 * only a single partition needed, if in that cases, the clustering expression would be ignored.
 * @param clustering the clustering keys
 */
private[sql] case class RepartitionKey(
                          clustering: Seq[Expression]) extends Gap

/**
 * Repartition the data according to the the new clustering expression, and we also need to
 * sort the data within the partition according to the clustering expression.
 * Notice: The clustering expressions should be the same with the sort keys.
 * @param clustering the clustering expression
 * @param sortKeys the sorting keys, should be the same with clustering expression, but with
 *                 sorting direction.
 */
private[sql] case class RepartitionKeyAndSort(
                          clustering: Seq[Expression],
                          sortKeys: Seq[SortOrder]) extends Gap


/**
 * Represent the output data distribution for a physical operator.
 *
 * @param numPartitions
 * @param clusterKeys
 * @param sortKeys
 * @param globalOrdered
 * @param additionalNullClusterKeyGenerated
 */
sealed case class Partitioning(
  /** the number of partitions that the data is split across */
  numPartitions: Option[Int] = None,

  /** the expressions that are used to key the partitioning. */
  clusterKeys: Seq[Expression] = Nil,

  /** the expression that are used to sort the data. */
  sortKeys: Seq[SortOrder] = Nil,

  /** work with `sortKeys` if the sorting cross or just within the partition. */
  globalOrdered: Boolean = false,

  /** to indicate if null clustering key will be generated. */
  additionalNullClusterKeyGenerated: Boolean = true) {

  def withNumPartitions(num: Int): Partitioning = {
    new Partitioning(
      numPartitions = Some(num),
      clusterKeys,
      sortKeys,
      globalOrdered,
      additionalNullClusterKeyGenerated)
  }

  def withClusterKeys(clusterKeys: Seq[Expression]): Partitioning = {
    new Partitioning(
      numPartitions,
      clusterKeys = clusterKeys,
      sortKeys,
      globalOrdered,
      additionalNullClusterKeyGenerated)
  }

  def withSortKeys(sortKeys: Seq[SortOrder], globalOrdering: Boolean = false): Partitioning = {
    if (globalOrdering) {
      new Partitioning(
        numPartitions,
        clusterKeys = sortKeys.map(_.child),
        sortKeys = sortKeys,
        globalOrdered = true,
        additionalNullClusterKeyGenerated)
    } else {
      new Partitioning(
        numPartitions,
        clusterKeys,
        sortKeys = sortKeys,
        globalOrdered = false,
        additionalNullClusterKeyGenerated)
    }
  }

  def withGlobalOrdered(globalOrdered: Boolean): Partitioning = {
    new Partitioning(
      numPartitions,
      clusterKeys,
      sortKeys,
      globalOrdered = globalOrdered,
      additionalNullClusterKeyGenerated)
  }

  def withAdditionalNullClusterKeyGenerated(nullClusterKeyGenerated: Boolean): Partitioning = {
    new Partitioning(
      numPartitions,
      clusterKeys,
      sortKeys,
      globalOrdered,
      additionalNullClusterKeyGenerated = nullClusterKeyGenerated)
  }

  /**
   * Compute the gap between the required data distribution and the existed data distribution.
   *
   * @param required the required data distribution
   * @return the gap that need to apply to the existed data, for its parent operator.
   */
  def gap(required: Distribution): Gap = required match {
    case UnspecifiedDistribution => NoGap
    case OrderedDistribution(ordering) if ordering == this.sortKeys && this.globalOrdered => NoGap
    case OrderedDistribution(ordering) => GlobalOrdering(ordering)
    case ClusteredDistribution(clustering, nullKeysSensitive, sortKeys) =>
      if (this.globalOrdered) {
        // Child is a global ordering partition (clustered by range), definitely requires
        // the repartitioning for a ClusteredDistribution
        if (sortKeys.nonEmpty) { // required sorting
          RepartitionKeyAndSort(clustering, sortKeys)
        } else {
          RepartitionKey(clustering)
        }
      } else {
        // Child is not a global ordering partition, probably a Clustered Partitioning or
        // UnspecifiedPartitioning
        if (this.clusterKeys == clustering) { // same distribution
          if (nullKeysSensitive) {
            // No NEW null cluster key generated from the child to be required
            // e.g. In GROUP BY clause, even the clustering key is the same, however,
            // if new null clustering key generated in child, we need to put all of the
            // null clustering into a single partition.
            if (this.additionalNullClusterKeyGenerated == false) {
              // No null clustering key generated
              if (sortKeys.isEmpty || sortKeys == this.sortKeys) {
                // No sorting required or the sorting keys are the same with current partitioning
                NoGap
              } else {
                // Sorting the data within the partition
                SortKeyWithinPartition(sortKeys)
              }
            } else {
              // child possible generate the null value for cluster keys,
              // we need to repartitioning the data
              if (sortKeys.nonEmpty) { // required sorting
                RepartitionKeyAndSort(clustering, sortKeys)
              } else {
                RepartitionKey(clustering)
              }
            }
          } else {
            // Don't care if null cluster key generated from the child.
            // E.g. In EQUAL-JOIN, we don't care about if null key should be in the same partition,
            // As we always consider the null key would be not equal to each other.
            if (sortKeys.isEmpty || sortKeys == this.sortKeys) {
              NoGap
            } else {
              SortKeyWithinPartition(sortKeys)
            }
          }
        } else { // not the same distribution
          if (sortKeys.nonEmpty) { // required sorting
            RepartitionKeyAndSort(clustering, sortKeys)
          } else {
            RepartitionKey(clustering)
          }
        }
      }
  }
}

// scalastyle:off
/******************************************************************/
/*             Helper utilities for the data partitioning         */
/******************************************************************/
// scalastyle:on

object UnknownPartitioning extends Partitioning

object HashPartition {
  type ReturnType = Option[(Seq[Expression], Int)] // (ClusteringKey, NumberOfPartition)

  def apply(clustering: Seq[Expression]): Partitioning = {
    UnknownPartitioning.withClusterKeys(clustering)
  }

  def unapply(part: Partitioning): ReturnType = {
    if (part.globalOrdered == false &&
        part.clusterKeys.nonEmpty &&
        part.sortKeys.isEmpty) {
      Some(part.clusterKeys, part.numPartitions.get)
    } else {
      None
    }
  }
}

object RangePartition {
  type ReturnType = Option[(Seq[SortOrder], Int)] // (Seq[SortOrder], NumberOfPartition)
  def apply(ordering: Seq[SortOrder]): Partitioning = {
    UnknownPartitioning.withSortKeys(ordering).withGlobalOrdered(true)
  }
  def unapply(part: Partitioning): ReturnType = {
    if (part.globalOrdered && part.sortKeys.nonEmpty) {
      Some(part.sortKeys, part.numPartitions.get)
    } else {
      None
    }
  }
}

object HashPartitionWithSort {
  // (Clustering Keys, Seq[SortOrder], NumberOfPartition)
  type ReturnType = Option[(Seq[Expression], Seq[SortOrder], Int)]

  def apply(clustering: Seq[Expression], sortKeys: Seq[SortOrder]): Partitioning = {
    UnknownPartitioning.withClusterKeys(clustering).withSortKeys(sortKeys)
  }

  def unapply(part: Partitioning): ReturnType = {
    if (part.globalOrdered == false &&
        part.clusterKeys.nonEmpty &&
        part.sortKeys.nonEmpty) {
      Some(part.clusterKeys, part.sortKeys, part.numPartitions.get)
    } else {
      None
    }
  }
}

object SinglePartition {
  def apply(): Partitioning = {
    UnknownPartitioning.withClusterKeys(Nil).withNumPartitions(1)
  }

  def unapply(part: Partitioning): Option[Int] =
    if (part.numPartitions.get == 1 || part.clusterKeys.isEmpty) {
      Some(1)
    } else {
      None
    }
}

