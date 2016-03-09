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

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toCommentSafeString
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, SortMergeJoin}
import org.apache.spark.sql.execution.metric.LongSQLMetricValue

/**
  * An interface for those physical operators that support codegen.
  */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: TungstenAggregate => "agg"
    case _: BroadcastHashJoin => "bhj"
    case _: SortMergeJoin => "smj"
    case _: PhysicalRDD => "rdd"
    case _ => nodeName.toLowerCase
  }

  /**
    * Creates a metric using the specified name.
    *
    * @return name of the variable representing the metric
    */
  def metricTerm(ctx: CodegenContext, name: String): String = {
    val metric = ctx.addReferenceObj(name, longMetric(name))
    val value = ctx.freshName("metricValue")
    val cls = classOf[LongSQLMetricValue].getName
    ctx.addMutableState(cls, value, s"$value = ($cls) $metric.localValue();")
    value
  }

  /**
    * Whether this SparkPlan support whole stage codegen or not.
    */
  def supportCodegen: Boolean = true

  /**
    * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
    */
  private var parent: CodegenSupport = null

  /**
    * Returns all the RDDs of InternalRow which generates the input rows.
    *
    * Note: right now we support up to two RDDs.
    */
  def upstreams(): Seq[RDD[InternalRow]]

  /**
    * Returns Java source code to process the rows from upstream.
    */
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    waitForSubqueries()
    s"""
       |/*** PRODUCE: ${toCommentSafeString(this.simpleString)} */
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
    *   while (!shouldStop() && hashmap.hasNext()) {
    *     row = hashmap.next();
    *     # build the aggregation results
    *     # create variables for results
    *     # call consume(), which will call parent.doConsume()
    *   }
    */
  protected def doProduce(ctx: CodegenContext): String

  /**
    * Consume the columns generated from current SparkPlan, call it's parent.
    */
  final def consume(ctx: CodegenContext, input: Seq[ExprCode], row: String = null): String = {
    if (input != null) {
      assert(input.length == output.length)
    }
    parent.consumeChild(ctx, this, input, row)
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
    * of evaluated variables, to prevent them to be evaluated twice..
    */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet): String = {
    var evaluateVars = ""
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code != "" && required.contains(attributes(i))) {
        evaluateVars += ev.code.trim + "\n"
        ev.code = ""
      }
    }
    evaluateVars
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Consume the columns generated from its child, call doConsume() or emit the rows.
   *
   * An operator could generate variables for the output, or a row, either one could be null.
   *
   * If the row is not null, we create variables to access the columns that are actually used by
   * current plan before calling doConsume().
   */
  def consumeChild(
      ctx: CodegenContext,
      child: SparkPlan,
      input: Seq[ExprCode],
      row: String = null): String = {
    ctx.freshNamePrefix = variablePrefix
    val inputVars =
      if (row != null) {
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        child.output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).gen(ctx)
        }
      } else {
        input
      }
    s"""
       |
       |/*** CONSUME: ${toCommentSafeString(this.simpleString)} */
       |${evaluateRequiredVariables(child.output, inputVars, usedInputs)}
       |${doConsume(ctx, inputVars)}
     """.stripMargin
  }

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
    */
  protected def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    throw new UnsupportedOperationException
  }
}


/**
  * InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
  *
  * This is the leaf node of a tree with WholeStageCodegen, is used to generate code that consumes
  * an RDD iterator of InternalRow.
  */
case class InputAdapter(child: SparkPlan) extends UnaryNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // Right now, InputAdapter is only used when there is one upstream.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    val exprs = output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, true))
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columns = exprs.map(_.gen(ctx))
    s"""
       | while (!shouldStop() && $input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, columns).trim}
       | }
     """.stripMargin
  }

  override def simpleString: String = "INPUT"

  override def treeChildren: Seq[SparkPlan] = Nil
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
  *  doExecute() --------->   upstreams() -------> upstreams() ------> execute()
  *     |
  *      ----------------->   produce()
  *                             |
  *                          doProduce()  -------> produce()
  *                                                   |
  *                                                doProduce()
  *                                                   |
  *                                                consume()
  *                        consumeChild() <-----------|
  *                             |
  *                          doConsume()
  *                             |
  *  consumeChild()  <-----  consume()
  *
  * SparkPlan A should override doProduce() and doConsume().
  *
  * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
  * used to generated code for BoundReference.
  */
case class WholeStageCodegen(child: SparkPlan) extends UnaryNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val references = ctx.references.toArray
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      /** Codegened pipeline for:
        * ${toCommentSafeString(child.treeString.trim)}
        */
      class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(scala.collection.Iterator inputs[]) {
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripExtraNewLines(source)
    // println(s"${CodeFormatter.format(cleanedSource)}")
    CodeGenerator.compile(cleanedSource)

    val rdds = child.asInstanceOf[CodegenSupport].upstreams()
    assert(rdds.size <= 2, "Up to two upstream RDDs can be supported")
    if (rdds.length == 1) {
      rdds.head.mapPartitions { iter =>
        val clazz = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = buffer.hasNext
          override def next: InternalRow = buffer.next()
        }
      }
    } else {
      // Right now, we support up to two upstreams.
      rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
        val clazz = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(Array(leftIter, rightIter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = buffer.hasNext
          override def next: InternalRow = buffer.next()
        }
      }
    }
  }

  override def upstreams(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def consumeChild(
      ctx: CodegenContext,
      child: SparkPlan,
      input: Seq[ExprCode],
      row: String = null): String = {

    if (row != null) {
      // There is an UnsafeRow already
      s"""
         |append($row.copy());
       """.stripMargin.trim
    } else {
      assert(input != null)
      if (input.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(input)
        // generate the code to create a UnsafeRow
        ctx.currentVars = input
        val code = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        s"""
           |$evaluateInputs
           |${code.code.trim}
           |append(${code.value}.copy());
         """.stripMargin.trim
      } else {
        // There is no columns
        s"""
           |append(unsafeRow);
         """.stripMargin.trim
      }
    }
  }

  override def innerChildren: Seq[SparkPlan] = {
    child :: Nil
  }

  private def collectInputs(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case InputAdapter(c) => c :: Nil
    case other => other.children.flatMap(collectInputs)
  }

  override def treeChildren: Seq[SparkPlan] = {
    collectInputs(child)
  }

  override def simpleString: String = "WholeStageCodegen"
}


/**
  * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
  */
private[sql] case class CollapseCodegenStages(sqlContext: SQLContext) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    case e: CaseWhen => e.shouldCodegen
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      val haveManyColumns = plan.output.length > 200
      !willFallback && !haveManyColumns
    case _ => false
  }

  /**
   * Inserts a InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = plan match {
    case j @ SortMergeJoin(_, _, _, left, right) =>
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
    case plan: CodegenSupport if supportCodegen(plan) =>
      WholeStageCodegen(insertInputAdapter(plan))
    case other =>
      other.withNewChildren(other.children.map(insertWholeStageCodegen))
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (sqlContext.conf.wholeStageEnabled) {
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
