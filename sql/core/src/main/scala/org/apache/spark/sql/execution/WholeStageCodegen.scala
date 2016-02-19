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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, BuildLeft, BuildRight}
import org.apache.spark.sql.execution.metric.LongSQLMetricValue

/**
  * An interface for those physical operators that support codegen.
  */
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _: TungstenAggregate => "agg"
    case _: BroadcastHashJoin => "join"
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
    * Returns the RDD of InternalRow which generates the input rows.
    */
  def upstream(): RDD[InternalRow]

  /**
    * Returns Java source code to process the rows from upstream.
    */
  def produce(ctx: CodegenContext, parent: CodegenSupport): String = {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    doProduce(ctx)
  }

  /**
    * Generate the Java source code to process, should be overrided by subclass to support codegen.
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
    *     # create varialbles for results
    *     # call consume(), wich will call parent.doConsume()
    *   }
    */
  protected def doProduce(ctx: CodegenContext): String

  /**
    * Consume the columns generated from current SparkPlan, call it's parent.
    */
  def consume(ctx: CodegenContext, input: Seq[ExprCode], row: String = null): String = {
    if (input != null) {
      assert(input.length == output.length)
    }
    parent.consumeChild(ctx, this, input, row)
  }

  /**
    * Consume the columns generated from it's child, call doConsume() or emit the rows.
    */
  def consumeChild(
      ctx: CodegenContext,
      child: SparkPlan,
      input: Seq[ExprCode],
      row: String = null): String = {
    ctx.freshNamePrefix = variablePrefix
    if (row != null) {
      ctx.currentVars = null
      ctx.INPUT_ROW = row
      val evals = child.output.zipWithIndex.map { case (attr, i) =>
        BoundReference(i, attr.dataType, attr.nullable).gen(ctx)
      }
      s"""
         | ${evals.map(_.code).mkString("\n")}
         | ${doConsume(ctx, evals)}
       """.stripMargin
    } else {
      doConsume(ctx, input)
    }
  }

  /**
    * Generate the Java source code to process the rows from child SparkPlan.
    *
    * This should be override by subclass to support codegen.
    *
    * For example, Filter will generate the code like this:
    *
    *   # code to evaluate the predicate expression, result is isNull1 and value2
    *   if (isNull1 || value2) {
    *     # call consume(), which will call parent.doConsume()
    *   }
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
case class InputAdapter(child: SparkPlan) extends LeafNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doPrepare(): Unit = {
    child.prepare()
  }

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def supportCodegen: Boolean = false

  override def upstream(): RDD[InternalRow] = {
    child.execute()
  }

  override def doProduce(ctx: CodegenContext): String = {
    val exprs = output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, true))
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columns = exprs.map(_.gen(ctx))
    s"""
       | while (input.hasNext()) {
       |   InternalRow $row = (InternalRow) input.next();
       |   ${columns.map(_.code).mkString("\n").trim}
       |   ${consume(ctx, columns).trim}
       |   if (shouldStop()) {
       |     return;
       |   }
       | }
     """.stripMargin
  }

  override def simpleString: String = "INPUT"
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
  *  doExecute() --------->   upstream() -------> upstream() ------> execute()
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
case class WholeStageCodegen(plan: CodegenSupport, children: Seq[SparkPlan])
  extends SparkPlan with CodegenSupport {

  override def supportCodegen: Boolean = false

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering

  override def doPrepare(): Unit = {
    plan.prepare()
  }

  override def doExecute(): RDD[InternalRow] = {
    val ctx = new CodegenContext
    val code = plan.produce(ctx, this)
    val references = ctx.references.toArray
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      /** Codegened pipeline for:
        * ${plan.treeString.trim}
        */
      class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripExtraNewLines(source)
    // println(s"${CodeFormatter.format(cleanedSource)}")
    CodeGenerator.compile(cleanedSource)

    plan.upstream().mapPartitions { iter =>

      val clazz = CodeGenerator.compile(source)
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.setInput(iter)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = buffer.hasNext
        override def next: InternalRow = buffer.next()
      }
    }
  }

  override def upstream(): RDD[InternalRow] = {
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
         | currentRows.add($row.copy());
       """.stripMargin
    } else {
      assert(input != null)
      if (input.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        // generate the code to create a UnsafeRow
        ctx.currentVars = input
        val code = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        s"""
           | ${code.code.trim}
           | currentRows.add(${code.value}.copy());
         """.stripMargin
      } else {
        // There is no columns
        s"""
           | currentRows.add(unsafeRow);
         """.stripMargin
      }
    }
  }

  private[sql] override def resetMetrics(): Unit = {
    plan.foreach(_.resetMetrics())
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(simpleString)
    builder.append("\n")

    plan.generateTreeString(depth + 2, lastChildren :+ false :+ true, builder)
    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(depth + 1, lastChildren :+ false, builder))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder)
    }

    builder
  }

  override def simpleString: String = "WholeStageCodegen"
}


/**
  * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
  */
private[sql] case class CollapseCodegenStages(sqlContext: SQLContext) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
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

  def apply(plan: SparkPlan): SparkPlan = {
    if (sqlContext.conf.wholeStageEnabled) {
      plan.transform {
        case plan: CodegenSupport if supportCodegen(plan) =>
          var inputs = ArrayBuffer[SparkPlan]()
          val combined = plan.transform {
            // The build side can't be compiled together
            case b @ BroadcastHashJoin(_, _, _, BuildLeft, _, left, right) =>
              b.copy(left = apply(left))
            case b @ BroadcastHashJoin(_, _, _, BuildRight, _, left, right) =>
              b.copy(right = apply(right))
            case p if !supportCodegen(p) =>
              val input = apply(p)  // collapse them recursively
              inputs += input
              InputAdapter(input)
          }.asInstanceOf[CodegenSupport]
          WholeStageCodegen(combined, inputs)
      }
    } else {
      plan
    }
  }
}
