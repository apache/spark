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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonFunction
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, RepartitionByExpression, UnaryNode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StructType}

// The logical operators of the RDD API of PySpark on Spark Connect (see `RDDRelation` in
// relations.proto). An RDD is a plan whose rows hold its elements, and whose partitions are the
// partitions of the RDD: these operators keep the partitions of their inputs as the RDDs of
// Spark Classic do, the empty partitions included, which the optimizer leaves alone since it does
// not know them. They read the columns of their inputs by position. The shuffles of RDDs are
// `RepartitionByExpression`s of `DirectShufflePartitionID`, below a `PythonRDDValues`.

/**
 * A Python function of the index and of the elements of each partition of `child`, which
 * [[PythonRDDMapPartitionsExec]] runs as the PythonRDD of Spark Classic runs it.
 *
 * @param func the command of the function, as PySpark pickles it for a PythonRDD.
 * @param isBarrier whether the partitions are computed in a barrier stage.
 * @param outputPairs whether the function writes keys and values in turn, which are the
 *                    `key` (int) and `value` (binary) columns of the output; otherwise the output
 *                    has one column, `value`.
 */
case class PythonRDDMapPartitions(
    func: PythonFunction,
    isBarrier: Boolean,
    outputPairs: Boolean,
    output: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val references: AttributeSet = child.outputSet

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def stringArgs: Iterator[Any] = Iterator(isBarrier, outputPairs, output)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDMapPartitions =
    copy(child = newChild)
}

object PythonRDDMapPartitions {
  def apply(
      func: PythonFunction,
      isBarrier: Boolean,
      outputPairs: Boolean,
      child: LogicalPlan): PythonRDDMapPartitions = {
    val output = if (outputPairs) {
      PythonRDDOperators.pairsOutput()
    } else {
      PythonRDDOperators.valueOutput()
    }
    PythonRDDMapPartitions(func, isBarrier, outputPairs, output, child)
  }
}

/**
 * The second column of the rows of `child`: the values of its keys and values. For instance, the
 * values of the pairs of a [[PythonRDDMapPartitions]] with `outputPairs`, which a
 * [[RepartitionByExpression]] of `DirectShufflePartitionID(key)` moves to the partitions of
 * their keys, as `RDD.partitionBy` of PySpark shuffles its batches. It keeps the partitions of a
 * shuffle that writes no rows, which adaptive query execution would replace below a `Project` by
 * an empty relation.
 */
case class PythonRDDValues(child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output.drop(1)

  override lazy val references: AttributeSet = child.outputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDValues =
    copy(child = newChild)
}

/**
 * The partitions of `children` one after the other, as `SparkContext.union` concatenates RDDs
 * that do not have the same partitioner. Unlike the `Union` of the DataFrames, it keeps the
 * children that have no rows.
 */
case class PythonRDDUnion(output: Seq[Attribute], children: Seq[LogicalPlan]) extends LogicalPlan {

  override lazy val references: AttributeSet = AttributeSet(children.flatMap(_.output))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): PythonRDDUnion = copy(children = newChildren)
}

/**
 * The partitions at the same index of `children`, which have the same number of partitions:
 * either their rows at the same position joined, as `RDD.zip` pairs the elements of two RDDs, or,
 * with `concatenate`, their rows one after the other, as `SparkContext.union` concatenates the
 * partitions of RDDs with the same partitioner.
 */
case class PythonRDDZipPartitions(
    concatenate: Boolean,
    output: Seq[Attribute],
    children: Seq[LogicalPlan])
  extends LogicalPlan {

  override lazy val references: AttributeSet = AttributeSet(children.flatMap(_.output))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def stringArgs: Iterator[Any] = Iterator(concatenate, output)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): PythonRDDZipPartitions = copy(children = newChildren)
}

/**
 * The Cartesian product of `left` and `right`, as `RDD.cartesian` computes it: their rows joined,
 * in a partition for each pair of partitions of `left` and `right`.
 */
case class PythonRDDCartesian(output: Seq[Attribute], left: LogicalPlan, right: LogicalPlan)
  extends BinaryNode {

  override lazy val references: AttributeSet = left.outputSet ++ right.outputSet

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): PythonRDDCartesian = copy(left = newLeft, right = newRight)
}

/** The partitions of `child` in `numPartitions` partitions, as `RDD.coalesce` groups them. */
case class PythonRDDCoalesce(numPartitions: Int, child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override lazy val references: AttributeSet = child.outputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDCoalesce =
    copy(child = newChild)
}

/**
 * The rows of local data, `child`, in `numSlices` partitions, sliced as
 * `SparkContext.parallelize` slices a sequence.
 */
case class PythonRDDParallelize(numSlices: Int, child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override lazy val references: AttributeSet = child.outputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDParallelize =
    copy(child = newChild)
}

/**
 * The rows of a DataFrame, `child`, pickled in batches as `DataFrame.rdd` of PySpark pickles
 * them (see `Dataset.javaToPython`): the output has one column, `value`.
 */
case class PythonRDDFromDataFrame(output: Seq[Attribute], child: LogicalPlan) extends UnaryNode {

  override lazy val references: AttributeSet = child.outputSet

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDFromDataFrame =
    copy(child = newChild)
}

object PythonRDDFromDataFrame {
  def apply(child: LogicalPlan): PythonRDDFromDataFrame =
    PythonRDDFromDataFrame(PythonRDDOperators.valueOutput(), child)
}

/**
 * A DataFrame of the rows that `child` pickles in batches, the internal values of the rows of
 * the output (lists or tuples, as `StructType.toInternal` of PySpark returns them), as
 * `SparkSession.createDataFrame` of PySpark creates it from an RDD (see
 * `SparkSession.applySchemaToPythonRDD`).
 */
case class PythonRDDToDataFrame(output: Seq[Attribute], child: LogicalPlan) extends UnaryNode {

  override lazy val references: AttributeSet = child.outputSet

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDToDataFrame =
    copy(child = newChild)
}

object PythonRDDToDataFrame {
  def apply(schema: StructType, child: LogicalPlan): PythonRDDToDataFrame =
    PythonRDDToDataFrame(DataTypeUtils.toAttributes(schema), child)
}

/**
 * The elements of the RDD `child`, in rows of two columns: `partition` (int), the index of a
 * partition, and `data` (binary), elements of that partition. `data` holds each row of `child`
 * as PythonRDD writes an element to Python (see `PythonRDD.writeIteratorToStream`), so that the
 * deserializer of the RDD in Python reads them. The rows of a partition hold its elements in
 * order, and the partitions are in the order of their indices.
 *
 * @param partitions the indices of the partitions to compute, as `SparkContext.runJob` takes
 *                   them, or `None` for all the partitions.
 */
case class PythonRDDElements(
    partitions: Option[Seq[Int]],
    output: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  override lazy val references: AttributeSet = child.outputSet

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override protected def stringArgs: Iterator[Any] = Iterator(partitions, output)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDElements =
    copy(child = newChild)
}

object PythonRDDElements {
  def apply(partitions: Option[Seq[Int]], child: LogicalPlan): PythonRDDElements = {
    PythonRDDElements(
      partitions,
      Seq(
        AttributeReference("partition", IntegerType, nullable = false)(),
        AttributeReference("data", BinaryType, nullable = false)()),
      child)
  }
}

/**
 * The rows of `child`, in a stage that runs with `profile`, as the stage of an RDD with the
 * resource profile that `RDD.withResources` sets in Spark Classic.
 */
case class PythonRDDWithResources(profile: ResourceProfile, child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override lazy val references: AttributeSet = child.outputSet

  override protected def stringArgs: Iterator[Any] = Iterator(profile.id)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonRDDWithResources =
    copy(child = newChild)
}

private[sql] object PythonRDDOperators {

  /** The output of an RDD whose elements are in one binary column. */
  def valueOutput(): Seq[Attribute] = Seq(AttributeReference("value", BinaryType)())

  /** The output of an RDD of keys and values, before a shuffle. */
  def pairsOutput(): Seq[Attribute] = Seq(
    AttributeReference("key", IntegerType, nullable = false)(),
    AttributeReference("value", BinaryType)())

  /** New attributes for the output of an operator that joins the columns of its inputs. */
  def joinedOutput(children: Seq[LogicalPlan]): Seq[Attribute] =
    children.flatMap(_.output).map(a => newAttribute(a.name, a.dataType))

  /** New attributes for the output of an operator that concatenates its inputs. */
  def concatenatedOutput(children: Seq[LogicalPlan]): Seq[Attribute] =
    children.head.output.map(a => newAttribute(a.name, a.dataType))

  private def newAttribute(name: String, dataType: DataType): Attribute =
    AttributeReference(name, dataType)()

  /**
   * The number of partitions of an RDD that these operators give without running a job, or
   * `None` when it depends on the planning of a DataFrame.
   */
  def numPartitions(plan: LogicalPlan): Option[Int] = plan match {
    case p: PythonRDDParallelize => Some(p.numSlices)
    // A repartition into a number of partitions, which adaptive query execution keeps.
    case p: RepartitionByExpression => p.optNumPartitions
    case p: PythonRDDMapPartitions => numPartitions(p.child)
    case p: PythonRDDValues => numPartitions(p.child)
    case p: PythonRDDElements => numPartitions(p.child)
    case p: PythonRDDWithResources => numPartitions(p.child)
    case p: PythonRDDUnion =>
      p.children.map(numPartitions).foldLeft(Option(0)) {
        case (Some(sum), Some(n)) => Some(sum + n)
        case _ => None
      }
    case p: PythonRDDZipPartitions => numPartitions(p.children.head)
    case p: PythonRDDCartesian =>
      for (l <- numPartitions(p.left); r <- numPartitions(p.right)) yield l * r
    case p: PythonRDDCoalesce =>
      // CoalescedRDD makes as many groups as asked, but never more than its input has
      // partitions; locality can leave fewer for inputs that have preferred locations.
      p.child match {
        case _: PythonRDDParallelize | _: PythonRDDUnion | _: PythonRDDZipPartitions |
            _: PythonRDDCartesian | _: PythonRDDCoalesce =>
          numPartitions(p.child).map(math.min(_, p.numPartitions))
        case _ => None
      }
    case _ => None
  }
}
