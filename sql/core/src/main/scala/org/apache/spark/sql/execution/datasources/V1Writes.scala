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

package org.apache.spark.sql.execution.datasources

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeMap, AttributeSet, BitwiseAnd, Cast, Expression, HiveHash, Literal, NamedExpression, Pmod, SortOrder, String2StringExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, RebalancePartitions, RepartitionOperation, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

trait V1WriteCommand extends DataWritingCommand {
  /**
   * Specify the static partitions of the V1 write command.
   */
  def staticPartitions: Map[String, String]

  /**
   * The number of static partition columns in `partitionColumns`.
   * Note that, the static partition must be ahead of partition columns.
   */
  final def numStaticPartitionCols: Int = staticPartitions.size

  /**
   * Specify the partition columns of the V1 write command.
   */
  def partitionColumns: Seq[Attribute]

  /**
   * Specify the required ordering for the V1 write command. `FileFormatWriter` will
   * add SortExec if necessary when the requiredOrdering is empty.
   */
  def requiredOrdering: Seq[SortOrder]

  /**
   * Replace the static partition spec for the V1 write command.
   */
  def withNewStaticPartitionSpec(partitionSpec: Map[String, String]): V1WriteCommand
}

/**
 * A rule that adds logical sorts to V1 data writing commands.
 */
object V1Writes extends Rule[LogicalPlan] with SQLConfHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.plannedWriteEnabled) {
      plan.transformDown {
        case write: V1WriteCommand =>
          val newQuery = prepareQuery(write, write.query)
          val attrMap = AttributeMap(write.query.output.zip(newQuery.output))
          val newWrite = write.withNewChildren(newQuery :: Nil).transformExpressions {
            case a: Attribute if attrMap.contains(a) =>
              a.withExprId(attrMap(a).exprId)
          }
          newWrite
      }
    } else {
      plan
    }
  }

  private def prepareQuery(write: V1WriteCommand, query: LogicalPlan): LogicalPlan = {
    val empty2NullPlan = if (hasEmptyToNull(query)) {
      query
    } else {
      val projectList = V1WritesUtils.convertEmptyToNull(query.output, write.partitionColumns)
      if (projectList.isEmpty) query else Project(projectList, query)
    }
    assert(empty2NullPlan.output.length == query.output.length)
    val attrMap = AttributeMap(query.output.zip(empty2NullPlan.output))

    // Rewrite the attribute references in the required ordering to use the new output.
    val requiredOrdering = write.requiredOrdering.map(_.transform {
      case a: Attribute => attrMap.getOrElse(a, a)
    }.asInstanceOf[SortOrder])
    val outputOrdering = query.outputOrdering
    // Check if the ordering is already matched to ensure the idempotency of the rule.
    val orderingMatched = if (requiredOrdering.length > outputOrdering.length) {
      false
    } else {
      requiredOrdering.zip(outputOrdering).forall {
        case (requiredOrder, outputOrder) => requiredOrder.semanticEquals(outputOrder)
      }
    }
    if (orderingMatched) {
      empty2NullPlan
    } else {
      Sort(requiredOrdering, global = false, empty2NullPlan)
    }
  }

  private def hasEmptyToNull(plan: LogicalPlan): Boolean = {
    plan.find {
      case p: Project => V1WritesUtils.hasEmptyToNull(p.projectList)
      case _ => false
    }.isDefined
  }
}

/**
 * This rule is used to eliminate dynamic partition to static partition for v1 writes if the
 * partition columns is foldable, so that we can avoid unnecessary sort for dynamic partition.
 *
 * For example, a pure SQL:
 * {{{
 *   INSERT INTO TABLE t1 PARTITION(p) SELECT c, 'a' as p FROM t2
 *   =>
 *   INSERT INTO TABLE t1 PARTITION(p='a') SELECT c FROM t2
 * }}}
 */
object EliminateV1DynamicPartitionWrites extends Rule[LogicalPlan] {

  @tailrec
  private def queryOutput(p: LogicalPlan): Seq[NamedExpression] = p match {
    case p: Project => p.projectList
    case f: Filter => queryOutput(f.child)
    case r: RepartitionOperation => queryOutput(r.child)
    case r: RebalancePartitions => queryOutput(r.child)
    case s: Sort => queryOutput(s.child)
    case l: LocalLimit => queryOutput(l.child)
    case l: GlobalLimit => queryOutput(l.child)
    case _ => Seq.empty
  }

  private def getPartitionSpecString(part: Any): String = {
    if (part == null) {
      null
    } else {
      assert(part.isInstanceOf[UTF8String])
      ExternalCatalogUtils.getPartitionSpecString(part.asInstanceOf[UTF8String].toString)
    }
  }

  private def tryEvalStaticPartition(named: NamedExpression): Option[(String, String)] = {
    named match {
      case Alias(l: Literal, name) =>
        Some((name, getPartitionSpecString(
          Cast(l, StringType, Option(conf.sessionLocalTimeZone)).eval())))
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.eliminateDynamicPartitionWrites) {
      return plan
    }

    val resolver = SparkSession.active.sessionState.analyzer.resolver
    plan.transformDown {
      case v1Writes: V1WriteCommand =>
        val output = queryOutput(v1Writes.query)

        // We can not infer a static partition which after a dynamic partition column,
        // for example:
        // INSERT INTO TABLE t PARTITION BY(p1, p2)
        // SELECT c, p1, 'a' as p2
        var previousStaticPartition = true
        val newStaticPartitionSpec = new mutable.HashMap[String, String]()
        val it = v1Writes.partitionColumns.drop(v1Writes.numStaticPartitionCols)
          .map(attr => output.find(o => resolver(attr.name, o.name))).iterator
        while (previousStaticPartition && it.hasNext) {
          it.next().flatMap(part => tryEvalStaticPartition(part)) match {
            case Some((name, partitionValue)) => newStaticPartitionSpec.put(name, partitionValue)
            case None => previousStaticPartition = false
          }
        }

        if (newStaticPartitionSpec.nonEmpty) {
          v1Writes.withNewStaticPartitionSpec(v1Writes.staticPartitions ++ newStaticPartitionSpec)
        } else {
          v1Writes
        }
    }
  }
}

object V1WritesUtils {

  /** A function that converts the empty string to null for partition values. */
  case class Empty2Null(child: Expression) extends UnaryExpression with String2StringExpression {
    override def convert(v: UTF8String): UTF8String = if (v.numBytes() == 0) null else v
    override def nullable: Boolean = true
    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      nullSafeCodeGen(ctx, ev, c => {
        s"""if ($c.numBytes() == 0) {
           |  ${ev.isNull} = true;
           |  ${ev.value} = null;
           |} else {
           |  ${ev.value} = $c;
           |}""".stripMargin
      })
    }

    override protected def withNewChildInternal(newChild: Expression): Empty2Null =
      copy(child = newChild)
  }

  def getWriterBucketSpec(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute],
      options: Map[String, String]): Option[WriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)

      if (options.getOrElse(BucketingUtils.optionForHiveCompatibleBucketWrite, "false") ==
        "true") {
        // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression.
        // Without the extra bitwise-and operation, we can get wrong bucket id when hash value of
        // columns is negative. See Hive implementation in
        // `org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#getBucketNumber()`.
        val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
        val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

        // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
        // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
        //
        // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
        // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
        val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
        WriterBucketSpec(bucketIdExpression, fileNamePrefix)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression = HashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        WriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
  }

  def getBucketSortColumns(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute]): Seq[Attribute] = {
    bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }
  }

  def getSortOrder(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      numStaticPartitionCols: Int = 0): Seq[SortOrder] = {
    require(partitionColumns.size >= numStaticPartitionCols)

    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val writerBucketSpec = V1WritesUtils.getWriterBucketSpec(bucketSpec, dataColumns, options)
    val sortColumns = V1WritesUtils.getBucketSortColumns(bucketSpec, dataColumns)
    // Static partition must to be ahead of dynamic partition
    val dynamicPartitionColumns = partitionColumns.drop(numStaticPartitionCols)

    if (SQLConf.get.maxConcurrentOutputFileWriters > 0 && sortColumns.isEmpty) {
      // Do not insert logical sort when concurrent output writers are enabled.
      Seq.empty
    } else {
      // We should first sort by dynamic partition columns, then bucket id, and finally sorting
      // columns.
      (dynamicPartitionColumns ++ writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns)
        .map(SortOrder(_, Ascending))
    }
  }

  def convertEmptyToNull(
      output: Seq[Attribute],
      partitionColumns: Seq[Attribute]): Seq[NamedExpression] = {
    val partitionSet = AttributeSet(partitionColumns)
    var needConvert = false
    val projectList: Seq[NamedExpression] = output.map {
      case p if partitionSet.contains(p) && p.dataType == StringType && p.nullable =>
        needConvert = true
        Alias(Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) projectList else Nil
  }

  def hasEmptyToNull(expressions: Seq[Expression]): Boolean = {
    expressions.exists(_.exists(_.isInstanceOf[Empty2Null]))
  }
}
