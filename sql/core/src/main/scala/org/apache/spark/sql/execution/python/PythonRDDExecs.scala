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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.spark.{JobArtifactSet, SparkException, TaskContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.{PythonFunction, PythonRDD, PythonRunner, SerDeUtil}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.rdd.{PartitionPruningRDD, RDD, UnionRDD}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BoundReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.joins.UnsafeCartesianRDD
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

// The physical operators of the logical operators in PythonRDDOperators.scala. Their RDDs have the
// partitions of the RDDs of Spark Classic: each partition of an operator's output is computed from
// the partitions at the same index of its inputs, as the RDDs of PySpark compute it.

/** The elements that PythonRDD writes to Python for the rows of an RDD. */
private[python] object PythonRDDElementConverter {

  /**
   * The element of a row of `schema`: the byte array of its column or, for several columns, the
   * pairs of the byte arrays of its columns nested to the left, as `RDD.zip` and `RDD.cartesian`
   * of RDDs of pairs make them, which PythonRDD writes as the byte arrays of the columns in turn.
   * A string is written as its UTF-8 bytes, as PythonRDD writes a string.
   */
  def apply(schema: StructType): InternalRow => Any = {
    if (schema.isEmpty) {
      throw SparkException.internalError("An RDD of the RDD API of PySpark has no column.")
    }
    val columns = schema.fields.zipWithIndex.map { case (field, i) => column(field.dataType, i) }
    if (columns.length == 1) {
      columns.head
    } else {
      row => columns.tail.foldLeft[Any](columns.head(row))((element, c) => (element, c(row)))
    }
  }

  private def column(dataType: DataType, ordinal: Int): InternalRow => Array[Byte] = {
    dataType match {
      case BinaryType => row => if (row.isNullAt(ordinal)) null else row.getBinary(ordinal)
      case _: StringType =>
        row => if (row.isNullAt(ordinal)) null else row.getUTF8String(ordinal).getBytes
      case other =>
        throw SparkException.internalError(
          s"An RDD of the RDD API of PySpark has binary or string columns, but got $other.")
    }
  }
}

/** Physical plan node of [[PythonRDDMapPartitions]]. */
case class PythonRDDMapPartitionsExec(
    func: PythonFunction,
    isBarrier: Boolean,
    outputPairs: Boolean,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override protected def doExecute(): RDD[InternalRow] = {
    val toElement = PythonRDDElementConverter(child.schema)
    val func = this.func
    val outputPairs = this.outputPairs
    val schema = this.schema
    val jobArtifactUUID = this.jobArtifactUUID
    val compute = (index: Int, iter: Iterator[InternalRow]) => {
      val runner = PythonRunner(func, jobArtifactUUID)
      // As PythonRDD.compute: the runner writes the elements as it writes those of any RDD.
      val elements = iter.map(toElement).asInstanceOf[Iterator[Array[Byte]]]
      val results = runner.compute(elements, index, TaskContext.get())
      val proj = UnsafeProjection.create(schema)
      if (outputPairs) {
        // As PairwiseRDD: the keys and the values in turn, a key being a long of 8 bytes.
        new Iterator[InternalRow] {
          override def hasNext: Boolean = results.hasNext

          override def next(): InternalRow = {
            val key = results.next()
            if (!results.hasNext) {
              throw SparkException.internalError("PairwiseRDD: unexpected value: a key alone")
            }
            proj(InternalRow(Utils.deserializeLongValue(key).toInt, results.next()))
          }
        }
      } else {
        results.map(value => proj(InternalRow(value)))
      }
    }
    val input = child.execute()
    if (isBarrier) {
      input.barrier().mapPartitionsWithIndex(compute)
    } else {
      input.mapPartitionsWithIndexInternal(compute)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDMapPartitionsExec =
    copy(child = newChild)
}

/** Physical plan node of [[PythonRDDValues]]. */
case class PythonRDDValuesExec(output: Seq[Attribute], child: SparkPlan) extends UnaryExecNode {

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    val value = child.output(1)
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(Seq(BoundReference(1, value.dataType, value.nullable)))
      iter.map(proj)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDValuesExec =
    copy(child = newChild)
}

/** Physical plan node of [[PythonRDDUnion]]. */
case class PythonRDDUnionExec(output: Seq[Attribute], children: Seq[SparkPlan]) extends SparkPlan {

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(children.map(_.outputPartitioning.numPartitions).sum)

  override protected def doExecute(): RDD[InternalRow] =
    new UnionRDD(sparkContext, children.map(_.execute()))

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): PythonRDDUnionExec = copy(children = newChildren)
}

/** Physical plan node of [[PythonRDDZipPartitions]]. */
case class PythonRDDZipPartitionsExec(
    concatenate: Boolean,
    output: Seq[Attribute],
    children: Seq[SparkPlan])
  extends SparkPlan {

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(children.head.outputPartitioning.numPartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    val inputs = children.map(_.execute())
    if (concatenate) {
      inputs.reduceLeft { (left, right) =>
        left.zipPartitions(right, preservesPartitioning = false)(_ ++ _)
      }
    } else {
      val Seq(leftSchema, rightSchema) = children.map(_.schema)
      inputs.head.zipPartitions(inputs(1), preservesPartitioning = false) { (left, right) =>
        val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
        // As RDD.zip: the partitions have the same number of rows.
        new Iterator[InternalRow] {
          override def hasNext: Boolean = (left.hasNext, right.hasNext) match {
            case (true, true) => true
            case (false, false) => false
            case _ => throw SparkCoreErrors.canOnlyZipRDDsWithSamePartitionSizeError()
          }

          override def next(): InternalRow = joiner.join(
            left.next().asInstanceOf[UnsafeRow],
            right.next().asInstanceOf[UnsafeRow])
        }
      }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): PythonRDDZipPartitionsExec =
    copy(children = newChildren)
}

/** Physical plan node of [[PythonRDDCartesian]], as CartesianProductExec computes it. */
case class PythonRDDCartesianExec(output: Seq[Attribute], left: SparkPlan, right: SparkPlan)
  extends BinaryExecNode {

  override def outputPartitioning: Partitioning = UnknownPartitioning(
    left.outputPartitioning.numPartitions * right.outputPartitioning.numPartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    val pair = new UnsafeCartesianRDD(
      left.execute().asInstanceOf[RDD[UnsafeRow]],
      right.execute().asInstanceOf[RDD[UnsafeRow]],
      conf.cartesianProductExecBufferInMemoryThreshold,
      conf.cartesianProductExecBufferSpillThreshold,
      conf.cartesianProductExecBufferSizeSpillThreshold)
    val leftSchema = left.schema
    val rightSchema = right.schema
    pair.mapPartitionsInternal { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(leftSchema, rightSchema)
      iter.map(r => joiner.join(r._1, r._2))
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): PythonRDDCartesianExec = copy(left = newLeft, right = newRight)
}

/** Physical plan node of [[PythonRDDParallelize]]. */
case class PythonRDDParallelizeExec(numSlices: Int, output: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

  override def outputPartitioning: Partitioning = UnknownPartitioning(numSlices)

  override protected def doExecute(): RDD[InternalRow] = {
    // As LocalTableScanExec, but in the partitions that SparkContext.parallelize slices.
    val rows = child.executeCollect().map(_.copy())
    sparkContext.parallelize(rows.toImmutableArraySeq, numSlices)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDParallelizeExec =
    copy(child = newChild)
}

/** Physical plan node of [[PythonRDDFromDataFrame]], as `Dataset.javaToPython` pickles rows. */
case class PythonRDDFromDataFrameExec(output: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    val inputSchema = child.schema
    val binaryAsBytes = conf.pysparkBinaryAsBytes
    val schema = this.schema
    child.execute().mapPartitionsInternal { iter =>
      EvaluatePython.registerPicklers()
      val proj = UnsafeProjection.create(schema)
      val rows = iter.map(EvaluatePython.toJava(_, inputSchema, binaryAsBytes))
      new SerDeUtil.AutoBatchedPickler(rows).map(batch => proj(InternalRow(batch)))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDFromDataFrameExec =
    copy(child = newChild)
}

/**
 * Physical plan node of [[PythonRDDToDataFrame]], as `SparkSession.applySchemaToPythonRDD`
 * reads the rows that PySpark pickles.
 */
case class PythonRDDToDataFrameExec(output: Seq[Attribute], child: SparkPlan)
  extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning =
    UnknownPartitioning(child.outputPartitioning.numPartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    val schema = this.schema
    val pickled = child.execute().map(_.getBinary(0))
    val objects = SerDeUtil.toJavaArray(SerDeUtil.pythonToJava(JavaRDD.fromRDD(pickled), true))
    objects.rdd.mapPartitions { iter =>
      val fromJava = EvaluatePython.makeFromJava(schema)
      val proj = UnsafeProjection.create(schema)
      iter.map(values => proj(fromJava(values).asInstanceOf[InternalRow]))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDToDataFrameExec =
    copy(child = newChild)
}

/** Physical plan node of [[PythonRDDElements]]. */
case class PythonRDDElementsExec(
    partitions: Option[Seq[Int]],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  /** The indices of the partitions to compute, in the order of the partitions. */
  private lazy val selectedPartitions: Option[Array[Int]] =
    partitions.map(_.distinct.sorted.toArray)

  override def outputPartitioning: Partitioning = UnknownPartitioning(
    selectedPartitions.map(_.length).getOrElse(child.outputPartitioning.numPartitions))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val toElement = PythonRDDElementConverter(child.schema)
    val schema = this.schema
    val indices = selectedPartitions
    val all = child.execute()
    val input = indices match {
      case Some(selected) =>
        val keep = selected.toSet
        PartitionPruningRDD.create(all, keep.contains)
      case None => all
    }
    val rowSize = PythonRDDElementsExec.ROW_SIZE
    input.mapPartitionsWithIndexInternal { (index, iter) =>
      val partitionIndex = indices.map(_(index)).getOrElse(index)
      val elements = iter.map(toElement)
      val proj = UnsafeProjection.create(schema)
      val buffer = new ByteArrayOutputStream()
      val out = new DataOutputStream(buffer)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = elements.hasNext

        override def next(): InternalRow = {
          buffer.reset()
          while (buffer.size() < rowSize && PythonRDD.writeNextElementToStream(elements, out)) {}
          out.flush()
          numOutputRows += 1
          proj(InternalRow(partitionIndex, buffer.toByteArray))
        }
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    val selected = partitions.map(p => s" partitions ${p.mkString(", ")}").getOrElse("")
    s"$nodeName$selected${truncatedString(output, "[", ",", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDElementsExec =
    copy(child = newChild)
}

object PythonRDDElementsExec {

  /** The size at which a row of `data` is complete, unless one element is larger. */
  val ROW_SIZE: Int = 1 << 20
}

/** Physical plan node of [[PythonRDDWithResources]]. */
case class PythonRDDWithResourcesExec(profile: ResourceProfile, child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def doExecute(): RDD[InternalRow] = {
    // The scheduler runs a stage with the resource profiles of its RDDs. This sets the profile on
    // a new RDD, since the RDD of the child can be in the plans of other stages too, as the RDD
    // of a reused exchange.
    child.execute().mapPartitionsInternal(iter => iter, preservesPartitioning = true)
      .withResources(profile)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): PythonRDDWithResourcesExec =
    copy(child = newChild)
}
