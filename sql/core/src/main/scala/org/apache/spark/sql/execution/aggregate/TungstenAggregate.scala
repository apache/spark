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
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
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

    val numOutput = metricTerm(ctx, "numOutputRows")
    s"""
       | if (!$initAgg) {
       |   $initAgg = true;
       |   $doAgg();
       |
       |   // output the result
       |   ${genResult.trim}
       |
       |   $numOutput.add(1);
       |   ${consume(ctx, resultVars).trim}
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
       | ${aggVals.map(_.code).mkString("\n").trim}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
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
  private var sorterTerm: String = _

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
    * Called by generated Java class to finish the aggregate and return a KVIterator.
    */
  def finishAggregate(
      hashMap: UnsafeFixedWidthAggregationMap,
      sorter: UnsafeKVExternalSorter): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val peakMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    metrics.incPeakExecutionMemory(peakMemory)
    // TODO: update data size and spill size

    if (sorter == null) {
      // not spilled
      return hashMap.iterator()
    }

    // merge the final hashMap into sorter
    sorter.merge(hashMap.destructAndCreateExternalSorter())
    hashMap.free()
    val sortedIter = sorter.sortedIterator()

    // Create a KVIterator based on the sorted iterator.
    new KVIterator[UnsafeRow, UnsafeRow] {

      // Create a MutableProjection to merge the rows of same key together
      val mergeExpr = declFunctions.flatMap(_.mergeExpressions)
      val mergeProjection = newMutableProjection(
        mergeExpr,
        bufferAttributes ++ declFunctions.flatMap(_.inputAggBufferAttributes),
        subexpressionEliminationEnabled)()
      val joinedRow = new JoinedRow()

      var currentKey: UnsafeRow = null
      var currentRow: UnsafeRow = null
      var nextKey: UnsafeRow = if (sortedIter.next()) {
        sortedIter.getKey
      } else {
        null
      }

      override def next(): Boolean = {
        if (nextKey != null) {
          currentKey = nextKey.copy()
          currentRow = sortedIter.getValue.copy()
          nextKey = null
          // use the first row as aggregate buffer
          mergeProjection.target(currentRow)

          // merge the following rows with same key together
          var findNextGroup = false
          while (!findNextGroup && sortedIter.next()) {
            val key = sortedIter.getKey
            if (currentKey.equals(key)) {
              mergeProjection(joinedRow(currentRow, sortedIter.getValue))
            } else {
              // We find a new group.
              findNextGroup = true
              nextKey = key
            }
          }

          true
        } else {
          false
        }
      }

      override def getKey: UnsafeRow = currentKey
      override def getValue: UnsafeRow = currentRow
      override def close(): Unit = {
        sortedIter.close()
      }
    }
  }

  /**
    * Generate the code for output.
    */
  private def generateResultCode(
      ctx: CodegenContext,
      keyTerm: String,
      bufferTerm: String,
      plan: String): String = {
    if (modes.contains(Final) || modes.contains(Complete)) {
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
        s"$joinerTerm = $plan.createUnsafeJoiner();")
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
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // create hashMap
    val thisPlan = ctx.addReferenceObj("plan", this)
    hashMapTerm = ctx.freshName("hashMap")
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    ctx.addMutableState(hashMapClassName, hashMapTerm, s"$hashMapTerm = $thisPlan.createHashMap();")
    sorterTerm = ctx.freshName("sorter")
    ctx.addMutableState(classOf[UnsafeKVExternalSorter].getName, sorterTerm, "")

    // Create a name for iterator from HashMap
    val iterTerm = ctx.freshName("mapIter")
    ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName, iterTerm, "")

    val doAgg = ctx.freshName("doAggregateWithKeys")
    ctx.addNewFunction(doAgg,
      s"""
        private void $doAgg() throws java.io.IOException {
          ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}

          $iterTerm = $thisPlan.finishAggregate($hashMapTerm, $sorterTerm);
        }
       """)

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val outputCode = generateResultCode(ctx, keyTerm, bufferTerm, thisPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
     if (!$initAgg) {
       $initAgg = true;
       $doAgg();
     }

     // output the result
     while ($iterTerm.next()) {
       $numOutput.add(1);
       UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
       UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
       $outputCode

       if (shouldStop()) return;
     }

     $iterTerm.close();
     if ($sorterTerm == null) {
       $hashMapTerm.free();
     }
     """
  }

  private def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {

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

    // generate hash code for key
    val hashExpr = Murmur3Hash(groupingExpressions, 42)
    ctx.currentVars = input
    val hashEval = BindReferences.bindReference(hashExpr, child.output).gen(ctx)

    val inputAttr = bufferAttributes ++ child.output
    ctx.currentVars = new Array[ExprCode](bufferAttributes.length) ++ input
    ctx.INPUT_ROW = buffer
    // TODO: support subexpression elimination
    val evals = updateExpr.map(BindReferences.bindReference(_, inputAttr).gen(ctx))
    val updates = evals.zipWithIndex.map { case (ev, i) =>
      val dt = updateExpr(i).dataType
      ctx.updateColumn(buffer, dt, i, ev, updateExpr(i).nullable)
    }

    val (checkFallback, resetCoulter, incCounter) = if (testFallbackStartsAt.isDefined) {
      val countTerm = ctx.freshName("fallbackCounter")
      ctx.addMutableState("int", countTerm, s"$countTerm = 0;")
      (s"$countTerm < ${testFallbackStartsAt.get}", s"$countTerm = 0;", s"$countTerm += 1;")
    } else {
      ("true", "", "")
    }

    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    s"""
     // generate grouping key
     ${keyCode.code.trim}
     ${hashEval.code.trim}
     UnsafeRow $buffer = null;
     if ($checkFallback) {
       // try to get the buffer from hash map
       $buffer = $hashMapTerm.getAggregationBufferFromUnsafeRow($key, ${hashEval.value});
     }
     if ($buffer == null) {
       if ($sorterTerm == null) {
         $sorterTerm = $hashMapTerm.destructAndCreateExternalSorter();
       } else {
         $sorterTerm.merge($hashMapTerm.destructAndCreateExternalSorter());
       }
       $resetCoulter
       // the hash map had be spilled, it should have enough memory now,
       // try  to allocate buffer again.
       $buffer = $hashMapTerm.getAggregationBufferFromUnsafeRow($key, ${hashEval.value});
       if ($buffer == null) {
         // failed to allocate the first page
         throw new OutOfMemoryError("No enough memory for aggregation");
       }
     }
     $incCounter

     // evaluate aggregate function
     ${evals.map(_.code).mkString("\n").trim}
     // update aggregate buffer
     ${updates.mkString("\n").trim}
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
