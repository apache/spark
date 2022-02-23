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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends JoinCodegenSupport {

  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // md:  应该是为了统一流和批所以统一了概念，即不使用build和probe table的概念，而是使用streamed（对应probe）
  //  和broadcast（对应build）；从下面可知，左表是probe表而右表是build表
  /** BuildRight means the right relation is the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      // md: 这里为什么是IdentityBroadcastMode？因为是BNLJ，为了NL的操作而没有hash key，所以需要传递原始格式而不是hash形态；
      //  同时，因为是BNLJ，所以build表的分布就是单分区形态（因为广播），而probe表则无所谓；
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  override def outputPartitioning: Partitioning = (joinType, buildSide) match {
    // md: 因为build表属于被broadcast而streamed表是按照分区来与build表做join的，所以如下这几种情况，对于join之后的relation，
    //  其partition信息是完全与streamed相关的；而如果leftOuter且build表为左表，无法继承之前的分区，也就破坏了join之后的分区情况
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSemi, BuildRight) | (LeftAnti, BuildRight) => streamed.outputPartitioning
    case _ => super.outputPartitioning
  }

  override def outputOrdering: Seq[SortOrder] = (joinType, buildSide) match {
    // md: 与partition情况类似，如果streamed表继续保持probe表的角色，那就可以继续继承之前的数据排序关系
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSemi, BuildRight) | (LeftAnti, BuildRight) => streamed.outputOrdering
    case _ => Nil
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      Predicate.create(condition.get, streamed.output ++ broadcast.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  /**
   * The implementation for InnerJoin.
   *
   * md: 比较简单，streamed按分区迭代，针对每一行数据，再迭代build表中的每一行，然后组合成一个新的行用于条件评估；
   */
  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    // md: 这里的streamedIter其实是单个分区下的数据迭代，是通过mapPartitionsInternal()方法
    //  来回调底层的RDD.compute(split)方式而产生的
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        // md: 为什么这里需要判断condition.isDefined而其他地方不判断？因为之类直接操作整个Iterator，通过条件判断直接跳过整体逻辑；
        //  虽然直接调用filter(condition)，但都属于无效方法调用
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  //   md: 也比较清晰，针对streamed表的每行数据（通过迭代器获取），与build表中去组合并；如果匹配某条数据，那就组合左右两个表的这
  //    两条数据形成新的行，再去校验join condition是否满足，然后输出；如果在build表中未匹配任何记录，那就只输出一行记录，包含左表所有的列值
  //    而右表对应的列的值全是null（通过一个高效数据结构标记一下N个列都是null即可）
  private def outerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      // md: 用来直接拼接build表的多个null值列
      val nulls = new GenericInternalRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        private var foundMatch: Boolean = false
        // the matched result row
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        private var nextIndex: Int = 0

        @scala.annotation.tailrec
        private def findNextMatch(): Boolean = {
          if (streamRow == null) {
            if (!streamedIter.hasNext) {
              return false
            }
            streamRow = streamedIter.next()
            nextIndex = 0
            // md: 用来标记整个build表里，有没有匹配到；因为在left outer join（right也一样）中左表的某行数据在整个build表中
            //  只要匹配到一条，就不会输出右表的null值；
            foundMatch = false
          }
          while (nextIndex < buildRows.length) {
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            nextIndex += 1
            if (boundCondition(resultRow)) {
              foundMatch = true
              return true
            }
          }
          if (!foundMatch) {
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext(): Boolean = {
          resultRow != null || findNextMatch()
        }
        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for LeftSemi and LeftAnti joins.
   */
  private def leftExistenceJoin(
      relation: Broadcast[Array[InternalRow]],
      exists: Boolean): RDD[InternalRow] = {
    buildSide match {
      // md: 这种join比较简单，streamed分区的数据直接从build表检索，对于exists发现一条数据即可，对于not exists需要build表中的数据
      //  都不满足条件才行
      case BuildRight =>
        streamed.execute().mapPartitionsInternal { streamedIter =>
          val buildRows = relation.value
          val joinedRow = new JoinedRow

          if (condition.isDefined) {
            streamedIter.filter(l =>
              // md: 把左右两个表的数据组合在一起，然后再评估是否满足条件，这就是correlated；当然，uncorrelated只是其中一种情况；
              //  对于not exists的场景，需要从build表中全部检索完才能做决定；
              buildRows.exists(r => boundCondition(joinedRow(l, r))) == exists
            )
          } else if (buildRows.nonEmpty == exists) {
            streamedIter
          } else {
            Iterator.empty
          }
        }
      // md: 因为现在left表判断是否出现于right表中而right表是stream表（也就是数据量较大），所以join条件为空时，如果stream表不为空，
      //  而同时SQL是'exists'判断，那就表明匹配成功，因此输出整个左表数据，而反之如果SQL是'not exists'结果stream表有数据，不满足条件，
      //  所以，输出空集合。对于单分区的stream表为空时，情况反过来；
      case BuildLeft if condition.isEmpty =>
        // If condition is empty, do not need to read rows from streamed side at all.
        // Only need to know whether streamed side is empty or not.
        val streamExists = !streamed.executeTake(1).isEmpty
        if (streamExists == exists) {
          sparkContext.makeRDD(relation.value)
        } else {
          sparkContext.emptyRDD
        }
      // md: 因为现在left表判断是否出现于right表中而right表是streamed表（也就是数据量较大），且join条件不空，所以不得不
      //  先把streamed表执行一遍，构建出build与stream表已经匹配的索引下标；
      case _ => // BuildLeft
        // md: buildLeft也就意味着，streamed table属于被build状态；而因为semijoin本身就是基于右表来判断左表
        val matchedBroadcastRows = getMatchedBroadcastRowsBitSet(streamed.execute(), relation)
        // md： 既然已经得到了build表与stream表之间满足join条件的位置下标，而这里只需要输出build表中匹配（或not exists表示不匹配）
        //  的记录，那就再便利和迭代一遍build表（已经被broadcast）即可；

        // md: 从这里可以看到，
        //  1）虽然大逻辑是在构建RDD（构建物理执行计划的代码，但未执行），但也有可能真实执行过，因为有些场景不得不提前计算，
        //  才能确保正确性；子查询也是一样
        //  2）很多子查询改写成join，在Spark中有一点好处就是，子查询是需要提前计算的，而join是可以放在一起边计算边预估，并配合codegen
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val buildRows = relation.value
        while (i < buildRows.length) {
          if (matchedBroadcastRows.get(i) == exists) {
            buf += buildRows(i).copy()
          }
          i += 1
        }
        sparkContext.makeRDD(buf)
    }
  }

  /**
   * The implementation for ExistenceJoin
   */
  private def existenceJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    buildSide match {
      // md: 可以看到，与前面leftSemi join && buildRight 逻辑基本类似；
      case BuildRight =>
        streamed.execute().mapPartitionsInternal { streamedIter =>
          val buildRows = relation.value
          val joinedRow = new JoinedRow

          if (condition.isDefined) {
            // md: 构建一个单值的数组
            val resultRow = new GenericInternalRow(Array[Any](null))
            // md: 这里稍微不一样的是，不管exist成功与否，stream表的结果都会输出；why？？？
            streamedIter.map { row =>
              val result = buildRows.exists(r => boundCondition(joinedRow(row, r)))
              resultRow.setBoolean(0, result)
              joinedRow(row, resultRow)
            }
          } else {
            val resultRow = new GenericInternalRow(Array[Any](buildRows.nonEmpty))
            streamedIter.map { row =>
              joinedRow(row, resultRow)
            }
          }
        }
        // md: 与前面leftSemi + buildLeft + 有条件场景基本类似，只不过多输出一列（标记存在性）
      case _ => // BuildLeft
        val matchedBroadcastRows = getMatchedBroadcastRowsBitSet(streamed.execute(), relation)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val buildRows = relation.value
        while (i < buildRows.length) {
          val result = new GenericInternalRow(Array[Any](matchedBroadcastRows.get(i)))
          // md: 这里稍微不一样的是，不管exist成功与否，stream表的结果都会输出；why？？？
          buf += new JoinedRow(buildRows(i).copy(), result)
          i += 1
        }
        sparkContext.makeRDD(buf)
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildLeft
   *   RightOuter with BuildRight
   *   FullOuter
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    val streamRdd = streamed.execute()
    // md: 这里是相当于先从build表（也就是对应outer侧的表）开始，寻找全局不匹配的行数据
    def notMatchedBroadcastRows: RDD[InternalRow] = {
      getMatchedBroadcastRowsBitSetRDD(streamRdd, relation)
        .repartition(1)
        .mapPartitions(iter => Seq(iter.fold(new BitSet(relation.value.length))(_ | _)).toIterator)
        .flatMap { matchedBroadcastRows =>
          val nulls = new GenericInternalRow(streamed.output.size)
          val buf: CompactBuffer[InternalRow] = new CompactBuffer()
          val joinedRow = new JoinedRow
          joinedRow.withLeft(nulls)
          var i = 0
          val buildRows = relation.value
          while (i < buildRows.length) {
            if (!matchedBroadcastRows.get(i)) {
              buf += joinedRow.withRight(buildRows(i)).copy()
            }
            i += 1
          }
          buf.iterator
        }
    }

    // md： 然后从streamed侧做一个单向的匹配outer匹配
    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
          i += 1
        }

        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    // md： 最后合并两部分数据；从上面可以看到，这种情况性能损失很大，因为stream表本身就很大，但是需要独立的遍历两次！！
    sparkContext.union(
      matchedStreamRows,
      notMatchedBroadcastRows
    )
  }

  /**
   * Get matched rows from broadcast side as a [[BitSet]].
   * Create a local [[BitSet]] for broadcast side on each RDD partition,
   * and merge all [[BitSet]]s together.
   */
  private def getMatchedBroadcastRowsBitSet(
      streamRdd: RDD[InternalRow],
      relation: Broadcast[Array[InternalRow]]): BitSet = {
    getMatchedBroadcastRowsBitSetRDD(streamRdd, relation)
      .fold(new BitSet(relation.value.length))(_ | _)
  }

  private def getMatchedBroadcastRowsBitSetRDD(
      streamRdd: RDD[InternalRow],
      relation: Broadcast[Array[InternalRow]]): RDD[BitSet] = {
    // md: 对整个streamRDD按照分区方式来规划好执行逻辑
    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matched.set(i)
          }
          i += 1
        }
      }
      Seq(matched).iterator
    }

    // md: 这里folder是为了：1）真正执行整个rdd，得到每个不同的分区与build表之间匹配成功的下标；2）把不同分区与build表之间匹配成功的bitset
    //  再用或操作合并在一起，从而相当从全局视角来判断build表中，哪些记录是与stream表能完成left semi join逻辑的（并满足join条件）；
    matchedBuildRows
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation)
      case (LeftSemi, _) =>
        leftExistenceJoin(broadcastedRelation, exists = true)
      case (LeftAnti, _) =>
        leftExistenceJoin(broadcastedRelation, exists = false)
      case (_: ExistenceJoin, _) =>
        existenceJoin(broadcastedRelation)
      case _ =>
        /**
         * LeftOuter with BuildLeft
         * RightOuter with BuildRight
         * FullOuter
         */
        defaultJoin(broadcastedRelation)
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  // md: 从spark issue(spark-31874)中可以看到，对于其他场景的join不好支持，因为：
  //  This PR is to add code-gen support for left semi / left anti BroadcastNestedLoopJoin
  //  (build side is right side). The execution code path for build left side cannot fit into
  //  whole stage code-gen framework, so only add the code-gen for build right side here.

  // md: 为什么说code-gen框架本身不支持呢？因为从对应逻辑但不是codegen的代码逻辑可以看到，
  //  比如leftOuter+buildLeft，需要比较复杂的前置逻辑，比如调用getMatchedBroadcastRowsBitSet()生成一个
  //  大的bitset集合（保存了streamed与build匹配的行号等）；这些逻辑如果都需要支持的话，就必须在codegen框架里
  //  提前定义好（关键，还要支持提前执行rdd等场景，需要在codegen中嵌入sparkContext），就codegen框架而言不是特别通用；

  // md: 而下面三种场景，都只需要对streamed表按照分区形式迭代，然后访问broadcast之后的build表就可以了；

  override def supportCodegen: Boolean = (joinType, buildSide) match {
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSemi | LeftAnti, BuildRight) => true
    case _ => false
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamed.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def needCopyResult: Boolean = true

  override def doProduce(ctx: CodegenContext): String = {
    streamed.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    (joinType, buildSide) match {
      case (_: InnerLike, _) => codegenInner(ctx, input)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) => codegenOuter(ctx, input)
      case (LeftSemi, BuildRight) => codegenLeftExistence(ctx, input, exists = true)
      case (LeftAnti, BuildRight) => codegenLeftExistence(ctx, input, exists = false)
      case _ =>
        // md: 通过[[supportCodegen]]方法来判断，所以不会有问题
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin code-gen should not take neither $joinType as the JoinType " +
          s"nor $buildSide as the BuildSide")
    }
  }

  /**
   * Returns a tuple of [[Broadcast]] side and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Array[InternalRow], String) = {
    // Create a name for broadcast side
    val broadcastArray = broadcast.executeBroadcast[Array[InternalRow]]()
    val broadcastTerm = ctx.addReferenceObj("broadcastTerm", broadcastArray)

    // Inline mutable state since not many join operations in a task
    val arrayTerm = ctx.addMutableState("InternalRow[]", "buildRowArray",
      v => s"$v = (InternalRow[]) $broadcastTerm.value();", forceInline = true)
    (broadcastArray.value, arrayTerm)
  }

  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // md: 在codegen时已经广播出去了
    val (_, buildRowArrayTerm) = prepareBroadcast(ctx)
    val (buildRow, checkCondition, buildVars) = getJoinCondition(ctx, input, streamed, broadcast)

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    val arrayIndex = ctx.freshName("arrayIndex")
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
       |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
       |  $checkCondition {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (buildRowArray, buildRowArrayTerm) = prepareBroadcast(ctx)
    val (buildRow, checkCondition, _) = getJoinCondition(ctx, input, streamed, broadcast)
    val buildVars = genOneSideJoinVars(ctx, buildRow, broadcast, setDefaultValue = true)

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    val arrayIndex = ctx.freshName("arrayIndex")
    val shouldOutputRow = ctx.freshName("shouldOutputRow")
    val foundMatch = ctx.freshName("foundMatch")
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (buildRowArray.isEmpty) {
      s"""
         |UnsafeRow $buildRow = null;
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else {
      s"""
         |boolean $foundMatch = false;
         |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
         |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
         |  boolean $shouldOutputRow = false;
         |  $checkCondition {
         |    $shouldOutputRow = true;
         |    $foundMatch = true;
         |  }
         |  if ($arrayIndex == $buildRowArrayTerm.length - 1 && !$foundMatch) {
         |    $buildRow = null;
         |    $shouldOutputRow = true;
         |  }
         |  if ($shouldOutputRow) {
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    }
  }

  private def codegenLeftExistence(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      exists: Boolean): String = {
    val (buildRowArray, buildRowArrayTerm) = prepareBroadcast(ctx)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (condition.isEmpty) {
      if (buildRowArray.nonEmpty == exists) {
        // Return streamed side if join condition is empty and
        // 1. build side is non-empty for LeftSemi join
        // or
        // 2. build side is empty for LeftAnti join.
        s"""
           |$numOutput.add(1);
           |${consume(ctx, input)}
         """.stripMargin
      } else {
        // Return nothing if join condition is empty and
        // 1. build side is empty for LeftSemi join
        // or
        // 2. build side is non-empty for LeftAnti join.
        ""
      }
    } else {
      val (buildRow, checkCondition, _) = getJoinCondition(ctx, input, streamed, broadcast)
      val foundMatch = ctx.freshName("foundMatch")
      val arrayIndex = ctx.freshName("arrayIndex")

      s"""
         |boolean $foundMatch = false;
         |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
         |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
         |  $checkCondition {
         |    $foundMatch = true;
         |    break;
         |  }
         |}
         |if ($foundMatch == $exists) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
     """.stripMargin
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastNestedLoopJoinExec =
    copy(left = newLeft, right = newRight)
}
