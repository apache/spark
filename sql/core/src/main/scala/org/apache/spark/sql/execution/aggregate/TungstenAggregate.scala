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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryNode, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.unsafe.KVIterator

case class TungstenAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(TungstenAggregate.supportsAggregate(aggregateBufferAttributes))

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  // This is for testing. We force TungstenAggregationIterator to fall back to sort-based
  // aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[Int] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt => Some(fallbackStartsAt.toInt)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    child.execute().mapPartitions { iter =>

      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new TungstenAggregationIterator(
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
            child.output,
            iter,
            testFallbackStartsAt,
            numInputRows,
            numOutputRows,
            dataSize,
            spillSize)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
    }
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  override def supportCodegen: Boolean = {
    // ImperativeAggregate is not supported right now
    !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def upstream(): RDD[InternalRow] = {
    child.asInstanceOf[CodegenSupport].upstream()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.gen(ctx)
      val initVars = s"""
         | $isNull = ${ev.isNull};
         | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }

    // generate variables for output
    val bufferAttrs = functions.flatMap(_.aggBufferAttributes)
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = functions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, bufferAttrs).gen(ctx)
      }
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, aggregateAttributes).gen(ctx)
      }
      (resultVars, s"""
        | ${aggResults.map(_.code).mkString("\n")}
        | ${resultVars.map(_.code).mkString("\n")}
       """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (bufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.gen(ctx))
      (resultVars, resultVars.map(_.code).mkString("\n"))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    ctx.addNewFunction(doAgg,
      s"""
         | private void $doAgg() throws java.io.IOException {
         |   // initialize aggregation buffer
         |   ${bufVars.map(_.code).mkString("\n")}
         |
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    s"""
       | if (!$initAgg) {
       |   $initAgg = true;
       |   $doAgg();
       |
       |   // output the result
       |   $genResult
       |
       |   ${consume(ctx, resultVars)}
       | }
     """.stripMargin
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }
    ctx.currentVars = bufVars ++ input
    // TODO: support subexpression elimination
    val aggVals = updateExpr.map(BindReferences.bindReference(_, inputAttrs).gen(ctx))
    // aggregate buffer should be updated atomic
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
       """.stripMargin
    }
    s"""
       | // do aggregate
       | ${aggVals.map(_.code).mkString("\n")}
       | // update aggregation buffer
       | ${updates.mkString("")}
     """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferAttributes = declFunctions.flatMap(_.aggBufferAttributes)
  private val bufferSchema = StructType.fromAttributes(bufferAttributes)

  // The name for HashMap
  private var hashMapTerm: String = _

  /**
    * This is called by generated Java class, should be public.
    */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get().taskMemoryManager(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
    )
  }

  /**
    * This is called by generated Java class, should be public.
    */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }


  /**
    * Update peak execution memory, called in generated Java class.
    */
  def updatePeakMemory(hashMap: UnsafeFixedWidthAggregationMap): Unit = {
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val metrics = TaskContext.get().taskMetrics()
    metrics.incPeakExecutionMemory(mapMemory)
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // create hashMap
    val thisPlan = ctx.addReferenceObj("plan", this)
    hashMapTerm = ctx.freshName("hashMap")
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    ctx.addMutableState(hashMapClassName, hashMapTerm, s"$hashMapTerm = $thisPlan.createHashMap();")

    // Create a name for iterator from HashMap
    val iterTerm = ctx.freshName("mapIter")
    ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName, iterTerm, "")

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val outputCode = if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output using resultExpressions
      ctx.currentVars = null
      ctx.INPUT_ROW = keyTerm
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).gen(ctx)
      }
      ctx.INPUT_ROW = bufferTerm
      val bufferVars = bufferAttributes.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).gen(ctx)
      }
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, bufferAttributes).gen(ctx)
      }
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).gen(ctx)
      }
      s"""
       ${keyVars.map(_.code).mkString("\n")}
       ${bufferVars.map(_.code).mkString("\n")}
       ${aggResults.map(_.code).mkString("\n")}
       ${resultVars.map(_.code).mkString("\n")}

       ${consume(ctx, resultVars)}
       """

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // This should be the last operator in a stage, we should output UnsafeRow directly
      val joinerTerm = ctx.freshName("unsafeRowJoiner")
      ctx.addMutableState(classOf[UnsafeRowJoiner].getName, joinerTerm,
        s"$joinerTerm = $thisPlan.createUnsafeJoiner();")
      val resultRow = ctx.freshName("resultRow")
      s"""
       UnsafeRow $resultRow = $joinerTerm.join($keyTerm, $bufferTerm);
       ${consume(ctx, null, resultRow)}
       """

    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val eval = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).gen(ctx)
      }
      s"""
       ${eval.map(_.code).mkString("\n")}
       ${consume(ctx, eval)}
       """
    }

    val doAgg = ctx.freshName("doAggregateWithKeys")
    ctx.addNewFunction(doAgg,
      s"""
        private void $doAgg() throws java.io.IOException {
          ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}

          $iterTerm = $hashMapTerm.iterator();
        }
       """)

    s"""
     if (!$initAgg) {
       $initAgg = true;
       $doAgg();
     }

     // output the result
     while ($iterTerm.next()) {
       UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
       UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
       $outputCode
     }

     $thisPlan.updatePeakMemory($hashMapTerm);
     $hashMapTerm.free();
     """
  }

  private def doConsumeWithKeys( ctx: CodegenContext, input: Seq[ExprCode]): String = {

    // create grouping key
    ctx.currentVars = input
    val keyCode = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val key = keyCode.value
    val buffer = ctx.freshName("aggBuffer")

    // only have DeclarativeAggregate
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    val inputAttr = bufferAttributes ++ child.output
    ctx.currentVars = new Array[ExprCode](bufferAttributes.length) ++ input
    ctx.INPUT_ROW = buffer
    // TODO: support subexpression elimination
    val evals = updateExpr.map(BindReferences.bindReference(_, inputAttr).gen(ctx))
    val updates = evals.zipWithIndex.map { case (ev, i) =>
      val dt = updateExpr(i).dataType
      ctx.updateColumn(buffer, dt, i, ev, updateExpr(i).nullable)
    }

    s"""
     // generate grouping key
     ${keyCode.code}
     UnsafeRow $buffer = $hashMapTerm.getAggregationBufferFromUnsafeRow($key);
     if ($buffer == null) {
       // failed to allocate the first page
       throw new OutOfMemoryError("No enough memory for aggregation");
     }

     // evaluate aggregate function
     ${evals.map(_.code).mkString("\n")}
     // update aggregate buffer
     ${updates.mkString("\n")}
     """
  }

  override def simpleString: String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = groupingExpressions.mkString("[", ",", "]")
        val functionString = allAggregateExpressions.mkString("[", ",", "]")
        val outputString = output.mkString("[", ",", "]")
        s"TungstenAggregate(key=$keyString, functions=$functionString, output=$outputString)"
      case Some(fallbackStartsAt) =>
        s"TungstenAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }
}

object TungstenAggregate {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
