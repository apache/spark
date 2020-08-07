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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.TimeUnit._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends HashJoin with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def output: Seq[Attribute] = super[ShuffledJoin].output

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil
    case _ => super.outputOrdering
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()

    val (isLookupAware, value) = if (joinType == FullOuter) {
      (true, Some(BindReferences.bindReferences(buildOutput, buildOutput)))
    } else {
      (false, None)
    }
    val relation = HashedRelation(
      iter,
      buildBoundKeys,
      taskMemoryManager = context.taskMemoryManager(),
      isLookupAware = isLookupAware,
      value = value)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      joinType match {
        case FullOuter => fullOuterJoin(streamIter, hashed, numOutputRows)
        case _ => join(streamIter, hashed, numOutputRows)
      }
    }
  }

  /**
   * Full outer shuffled hash join has three steps:
   * 1. Construct hash relation from build side,
   *    with extra boolean value at the end of row to track look up information
   *    (done in `buildHashedRelation`).
   * 2. Process rows from stream side by looking up hash relation,
   *    and mark the matched rows from build side be looked up.
   * 3. Process rows from build side by iterating hash relation,
   *    and filter out rows from build side being looked up already.
   */
  private def fullOuterJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val joinRow = new JoinedRow
    val (joinRowWithStream, joinRowWithBuild) = {
      buildSide match {
        case BuildLeft => (joinRow.withRight _, joinRow.withLeft _)
        case BuildRight => (joinRow.withLeft _, joinRow.withRight _)
      }
    }
    val joinKeys = streamSideKeyGenerator()
    val buildRowGenerator = UnsafeProjection.create(buildOutput, buildOutput)
    val buildNullRow = new GenericInternalRow(buildOutput.length)
    val streamNullRow = new GenericInternalRow(streamedOutput.length)

    def markRowLookedUp(row: UnsafeRow): Unit =
      row.setBoolean(row.numFields() - 1, true)

    // Process stream side with looking up hash relation
    val streamResultIter =
      if (hashedRelation.keyIsUnique) {
        streamIter.map { srow =>
          joinRowWithStream(srow)
          val keys = joinKeys(srow)
          if (keys.anyNull) {
            joinRowWithBuild(buildNullRow)
          } else {
            val matched = hashedRelation.getValue(keys)
            if (matched != null) {
              val buildRow = buildRowGenerator(matched)
              if (boundCondition(joinRowWithBuild(buildRow))) {
                markRowLookedUp(matched.asInstanceOf[UnsafeRow])
                joinRow
              } else {
                joinRowWithBuild(buildNullRow)
              }
            } else {
              joinRowWithBuild(buildNullRow)
            }
          }
        }
      } else {
        streamIter.flatMap { srow =>
          joinRowWithStream(srow)
          val keys = joinKeys(srow)
          if (keys.anyNull) {
            Iterator.single(joinRowWithBuild(buildNullRow))
          } else {
            val buildIter = hashedRelation.get(keys)
            new RowIterator {
              private var found = false
              override def advanceNext(): Boolean = {
                while (buildIter != null && buildIter.hasNext) {
                  val matched = buildIter.next()
                  val buildRow = buildRowGenerator(matched)
                  if (boundCondition(joinRowWithBuild(buildRow))) {
                    markRowLookedUp(matched.asInstanceOf[UnsafeRow])
                    found = true
                    return true
                  }
                }
                if (!found) {
                  joinRowWithBuild(buildNullRow)
                  found = true
                  return true
                }
                false
              }
              override def getRow: InternalRow = joinRow
            }.toScala
          }
        }
      }

    // Process build side with filtering out rows looked up already
    val buildResultIter = hashedRelation.values().flatMap { brow =>
      val unsafebrow = brow.asInstanceOf[UnsafeRow]
      val isLookup = unsafebrow.getBoolean(unsafebrow.numFields() - 1)
      if (!isLookup) {
        val buildRow = buildRowGenerator(unsafebrow)
        joinRowWithBuild(buildRow)
        joinRowWithStream(streamNullRow)
        Some(joinRow)
      } else {
        None
      }
    }

    val resultProj = UnsafeProjection.create(output, output)
    (streamResultIter ++ buildResultIter).map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  // TODO(SPARK-32567): support full outer shuffled hash join code-gen
  override def supportCodegen: Boolean = {
    joinType != FullOuter
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  override def needCopyResult: Boolean = true

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[1]);", forceInline = true)
    HashedRelationInfo(relationTerm, keyIsUnique = false, isEmpty = false)
  }
}
