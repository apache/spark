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

package org.apache.spark.sql.execution

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{broadcast, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * An interface for those physical operators that support codegen.
 */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: HashAggregateExec => "agg"
    case _: BroadcastHashJoinExec => "bhj"
    case _: SortMergeJoinExec => "smj"
    case _: RDDScanExec => "rdd"
    case _: DataSourceScanExec => "scan"
    case _ => nodeName.toLowerCase
  }

  /**
   * Creates a metric using the specified name.
   *
   * @return name of the variable representing the metric
   */
  def metricTerm(ctx: CodegenContext, name: String): String = {
    ctx.addReferenceObj(name, longMetric(name))
  }

  /**
   * Whether this SparkPlan support whole stage codegen or not.
   */
  def supportCodegen: Boolean = true

  /**
   * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
   */
  protected var parent: CodegenSupport = null

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * Note: right now we support up to two RDDs.
   */
  def inputRDDs(): Seq[RDD[InternalRow]]

  /**
   * Returns Java source code to process the rows from input RDD.
   */
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString}")}
       |${doProduce(ctx)}
     """.stripMargin
  }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   *   if (!initialized) {
   *     # create a hash map, then build the aggregation hash map
   *     # call child.produce()
   *     initialized = true;
   *   }
   *   while (hashmap.hasNext()) {
   *     row = hashmap.next();
   *     # build the aggregation results
   *     # create variables for results
   *     # call consume(), which will call parent.doConsume()
   *      if (shouldStop()) return;
   *   }
   */
  protected def doProduce(ctx: CodegenContext): String

  /**
   * Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
   */
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    val inputVars =
      if (row != null) {
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)
        }
      } else {
        assert(outputVars != null)
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeRow, so we should copy them
        outputVars.map(_.copy())
      }

    val rowVar = if (row != null) {
      ExprCode("", "false", row)
    } else {
      if (outputVars.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(outputVars)
        // generate the code to create a UnsafeRow
        ctx.INPUT_ROW = row
        ctx.currentVars = outputVars
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        val code = s"""
          |$evaluateInputs
          |${ev.code.trim}
         """.stripMargin.trim
        ExprCode(code, "false", ev.value)
      } else {
        // There is no columns
        ExprCode("", "false", "unsafeRow")
      }
    }

    ctx.freshNamePrefix = parent.variablePrefix
    val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs)
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |$evaluated
       |${parent.doConsume(ctx, inputVars, rowVar)}
     """.stripMargin
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent
   * them to be evaluated twice.
   */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code != "").map(_.code.trim).mkString("\n")
    variables.foreach(_.code = "")
    evaluate
  }

  /**
   * Returns source code to evaluate the variables for required attributes, and clear the code
   * of evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code != "" && required.contains(attributes(i))) {
        evaluateVars.append(ev.code.trim + "\n")
        ev.code = ""
      }
    }
    evaluateVars.toString()
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Generate the Java source code to process the rows from child SparkPlan.
   *
   * This should be override by subclass to support codegen.
   *
   * For example, Filter will generate the code like this:
   *
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (isNull1 || !value2) continue;
   *   # call consume(), which will call parent.doConsume()
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }
}


/**
 * InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
 *
 * This is the leaf node of a tree with WholeStageCodegen that is used to generate code
 * that consumes an RDD iterator of InternalRow.
 */
case class InputAdapter(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // Right now, InputAdapter is only used when there is one input RDD.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val row = ctx.freshName("row")
    s"""
       | while ($input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = ""): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "")
  }
}

object WholeStageCodegenExec {
  val PIPELINE_DURATION_METRIC = "duration"
}

/**
 * WholeStageCodegen compile a subtree of plans that support codegen together into single Java
 * function.
 *
 * Here is the call graph of to generate Java source (plan A support codegen, but plan B does not):
 *
 *   WholeStageCodegen       Plan A               FakeInput        Plan B
 * =========================================================================
 *
 * -> execute()
 *     |
 *  doExecute() --------->   inputRDDs() -------> inputRDDs() ------> execute()
 *     |
 *     +----------------->   produce()
 *                             |
 *                          doProduce()  -------> produce()
 *                                                   |
 *                                                doProduce()
 *                                                   |
 *                         doConsume() <--------- consume()
 *                             |
 *  doConsume()  <--------  consume()
 *
 * SparkPlan A should override doProduce() and doConsume().
 *
 * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
 * used to generated code for BoundReference.
 */
case class WholeStageCodegenExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegenExec.PIPELINE_DURATION_METRIC))

  /**
   * Generates code for this subtree.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")}
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  override def doExecute(): RDD[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case e: Exception if !Utils.isTesting && sqlContext.conf.wholeStageFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for this plan:\n $treeString")
        return child.execute()
    }
    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime")

    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()
    new WholeStageCodegenRDD(sqlContext.sparkContext, cleanedSource,
      references, durationMs, rdds)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val doCopy = if (ctx.copyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
      |${row.code}
      |append(${row.value}$doCopy);
     """.stripMargin.trim
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = ""): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 */
case class CollapseCodegenStages(conf: SQLConf) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case u: UserDefinedType[_] => numOfNestedFields(u.sqlType)
    case _ => 1
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      val hasTooManyOutputFields =
        numOfNestedFields(plan.schema) > conf.wholeStageMaxNumFields
      val hasTooManyInputFields =
        plan.children.map(p => numOfNestedFields(p.schema)).exists(_ > conf.wholeStageMaxNumFields)
      !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
    case _ => false
  }

  /**
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = plan match {
    case j @ SortMergeJoinExec(_, _, _, _, left, right) if j.supportCodegen =>
      // The children of SortMergeJoin should do codegen separately.
      j.copy(left = InputAdapter(insertWholeStageCodegen(left)),
        right = InputAdapter(insertWholeStageCodegen(right)))
    case p if !supportCodegen(p) =>
      // collapse them recursively
      InputAdapter(insertWholeStageCodegen(p))
    case p =>
      p.withNewChildren(p.children.map(insertInputAdapter))
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = plan match {
    // For operators that will output domain object, do not insert WholeStageCodegen for it as
    // domain object can not be written into unsafe row.
    case plan if plan.output.length == 1 &&
      plan.output.head.dataType.isInstanceOf[ObjectType] =>
      plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
    case plan: CodegenSupport => if (supportCodegen(plan)) {
      WholeStageCodegenExec(insertInputAdapter(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertInputAdapter))
    }
    case other =>
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled) {
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}

class WholeStageCodegenRDD(sc: SparkContext, var source: CodeAndComment,
    var references: Array[Any], var durationMs: SQLMetric,
    inputRDDs: Seq[RDD[InternalRow]])
    extends ZippedPartitionsBaseRDD[InternalRow](sc, inputRDDs)
        with Serializable with KryoSerializable {

  override def getPartitions: Array[Partition] = {
    if (rdds.length == 1) rdds.head.partitions
    else super.getPartitions
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    if (rdds.length == 1) rdds.head.preferredLocations(s)
    else s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def compute(split: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    val clazz = CodeGenerator.compile(source)
    val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
    if (rdds.length == 1) {
      buffer.init(split.index, Array(rdds.head.iterator(split, context)
          .asInstanceOf[Iterator[InternalRow]]))
    } else {
      val zippedPartition = split.asInstanceOf[ZippedPartitionsPartition]
      val partitions = zippedPartition.partitions
      val iterators = new Array[Iterator[InternalRow]](partitions.length)
      for (i <- partitions.indices) {
        iterators(i) = rdds(i).iterator(partitions(i), context)
            .asInstanceOf[Iterator[InternalRow]]
      }
      buffer.init(zippedPartition.index, iterators)
    }
    new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        val v = buffer.hasNext
        if (!v) durationMs += buffer.durationMs()
        v
      }
      override def next: InternalRow = buffer.next()
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(_id)

    // write CodeAndComment
    output.writeInt(source.hashCode())
    output.writeString(source.body)
    val comment = source.comment
    output.writeVarInt(comment.size, true)
    for ((k, v) <- comment) {
      output.writeString(k)
      output.writeString(v)
    }

    val refsLen = if (references != null) references.length else 0
    output.writeVarInt(refsLen, true)
    var i = 0
    while (i < refsLen) {
      kryo.writeClassAndObject(output, references(i))
      i += 1
    }
    durationMs.write(kryo, output)

    output.writeVarInt(rdds.length, true)
    for (rdd <- rdds) {
      kryo.writeClassAndObject(output, rdd)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    _id = input.readInt()
    storageLevel = StorageLevel.NONE
    checkpointData = None

    val hash = input.readInt()
    val body = input.readString()
    var commentSize = input.readVarInt(true)
    val comment = new scala.collection.mutable.HashMap[String, String]()
    while (commentSize > 0) {
      val k = input.readString()
      val v = input.readString()
      comment.put(k, v)
      commentSize -= 1
    }
    source = new CodeAndComment(body, comment)
    source.hash = hash

    val refsLen = input.readVarInt(true)
    if (refsLen > 0) {
      references = new Array[Any](refsLen)
      var i = 0
      while (i < refsLen) {
        references(i) = kryo.readClassAndObject(input)
        i += 1
      }
    } else {
      references = null
    }
    durationMs = new SQLMetric(null)
    durationMs.read(kryo, input)

    val rddsBuilder = IndexedSeq.newBuilder[RDD[InternalRow]]
    var rddsLen = input.readVarInt(true)
    while (rddsLen > 0) {
      rddsBuilder += kryo.readClassAndObject(input).asInstanceOf[RDD[InternalRow]]
      rddsLen -= 1
    }
    rdds = rddsBuilder.result()
  }
}
