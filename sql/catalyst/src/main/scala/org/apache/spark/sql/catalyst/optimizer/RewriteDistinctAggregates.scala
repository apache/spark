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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.{immutable, mutable}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, _}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * This rule rewrites an aggregate query with distinct aggregations into an expanded double
 * aggregation in which the regular aggregation expressions and every distinct clause is aggregated
 * in a separate group. The results are then combined in a second aggregate. In addition,
 * this rule applies 'merged column' and 'bit vector' tricks to reduce columns and rows
 * in the expand.
 *
 * First example: query without filter clauses (in scala):
 * {{{
 *   val data = Seq(
 *     ("a", "ca1", "cb1", 10, 1),
 *     ("a", "ca1", "cb2", 5, 2),
 *     ("a", "ca1", "cb3", 5, 3),
 *     ("b", "ca1", "cb1", 13, 4))
 *     .toDF("key", "cat1", "cat2", "value", "id")
 *   data.createOrReplaceTempView("data")
 *
 *   val agg = data.groupBy($"key")
 *     .agg(
 *       count_distinct($"cat1").as("cat1_cnt"),
 *       count_distinct($"cat2").as("cat2_cnt"),
 *       sum($"value").as("total"))
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1),
 *                 COUNT(DISTINCT 'cat2),
 *                 sum('value)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [count('merged_string_1) FILTER (WHERE 'gid = 1),
 *                 count('merged_string_1) FILTER (WHERE 'gid = 2),
 *                 first('sum(value)) ignore nulls FILTER (WHERE 'gid = 0)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'merged_string_1, 'gid]
 *      functions = [sum('value)]
 *      output = ['key, 'merged_string_1, 'sum(value)])
 *     Expand(
 *        projections = [('key, null, 1, 'value),
 *                       ('key, 'cat1, 2, null),
 *                       ('key, 'cat2, 3, null)]
 *        output = ['key, 'merged_string_1, 'gid, 'value])
 *       LocalTableScan [...]
 * }}}
 *
 * Second example: aggregate function with distinct and filter clauses (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) FILTER (WHERE id > 1) as cat1_cnt,
 *     COUNT(DISTINCT cat2) FILTER (WHERE id > 2) as cat2_cnt,
 *     SUM(value) FILTER (WHERE id > 3) AS total
 *   FROM
 *     data
 *   GROUP BY
 *     key
 * }}}
 *
 * This translates to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [COUNT(DISTINCT 'cat1) FILTER (WHERE 'id > 1),
 *                 COUNT(DISTINCT 'cat2) FILTER (WHERE 'id > 2),
 *                 sum('value) FILTER (WHERE 'id > 3)]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [
 *        count('merged_string_1)
 *          FILTER (WHERE (('gid = 1) AND NOT (('filter_vector_1 & 1) = 0))),
 *        count('merged_string_1)
 *          FILTER (WHERE (('gid = 2) AND NOT (('filter_vector_1 & 1) = 0))),
 *        first('(sum(value) FILTER (WHERE (id > 3))) ignore nulls)) FILTER (WHERE 'gid = 0)
 *    ]
 *    output = ['key, 'cat1_cnt, 'cat2_cnt, 'total])
 *   Aggregate(
 *      key = ['key, 'merged_string_1, 'gid]
 *      functions = [bit_or('filter_vector_1), sum('value) FILTER (WHERE 'id > 3)]
 *      output = ['key, 'merged_string_1, 'gid, 'bit_or(filter_vector_1),
 *      '(sum(value) FILTER (WHERE (id > 3)))])
 *     Expand(
 *        projections = [('key, null, 0, null, 'value, 'id),
 *                       ('key, 'cat1, 1, if (('id > 1)) 1 else 0, null, null),
 *                       ('key, 'cat2, 2, if (('id > 2)) 1 else 0, null, null)]
 *        output = ['key, 'merged_string_1, 'gid, 'filter_vector_1, 'value, 'id])
 *       LocalTableScan [...]
 * }}}
 *
 * The rule does the following things here:
 * 1. Expand the data. There are three aggregation groups in this query:
 *    i. the non-distinct group;
 *    ii. the distinct 'cat1 group;
 *    iii. the distinct 'cat2 group.
 *    An expand operator is inserted to expand the child data for each group. The expand will null
 *    out all unused columns for the given group; this must be done in order to ensure correctness
 *    later on. Groups can by identified by a group id (gid) column added by the expand operator.
 *    If distinct group exists filter clause, the expand will calculate the filter and output it's
 *    result (e.g. vector_1) which will be used to calculate the global
 *    vector (e.g. bit_or(vector_1)). One bit of vector indicates one filter clause result.
 * 2. De-duplicate the distinct paths and aggregate the non-aggregate path. The group by clause of
 *    this aggregate consists of the original group by clause, all the requested distinct columns
 *    and the group id. Both de-duplication of distinct column and the aggregation of the
 *    non-distinct group take advantage of the fact that we group by the group id (gid) and that we
 *    have nulled out all non-relevant columns the given group. If distinct group exists filter
 *    clause, we will use bit_or to aggregate the results (e.g. vector_1) of the vector output
 *    in the previous step. These aggregate will output the global
 *    vector (e.g. bit_or(vector_1)).
 * 3. Aggregating the distinct groups and combining this with the results of the non-distinct
 *    aggregation. In this step we use the group id and the global condition to filter the inputs
 *    for the aggregate functions. If the global vector's (e.g. bit_or(vector_1)) corresponding bit
 *    equals to 1, it means at least one row of a distinct value satisfies the filter. This distinct
 *    value should be included in the aggregate function. The result of the non-distinct
 *    group are 'aggregated' by using the first operator, it might be more elegant to
 *    use the native UDAF merge mechanism for this in the future.
 * This rule duplicates the input data by two or more times (# distinct groups + an optional
 * non-distinct group). This will put quite a bit of memory pressure of the used aggregate and
 * exchange operators. Keeping the number of distinct groups as low as possible should be priority,
 * we could improve this in the current rule by applying more advanced expression canonicalization
 * techniques.
 *
 * Third example: compare the difference between the original expand rewriting
 * and the new expand rewriting with 'merged column' and 'bit vector' (in sql):
 * {{{
 *   SELECT
 *     COUNT(DISTINCT cat1) FILTER (WHERE id > 1) as cat1_filter_cnt_dist,
 *     COUNT(DISTINCT cat2) FILTER (WHERE id > 2) as cat2_filter_cnt_dist,
 *     COUNT(DISTINCT IF(value > 5, cat1, null)) as cat1_if_cnt_dist,
 *     COUNT(DISTINCT id) as id_cnt_dist,
 *     SUM(DISTINCT value) as id_sum_dist
 *   FROM
 *     data
 *   GROUP BY
 *     key
 * }}}
 *
 * Original expand rule translates to the following (pseudo) logical plan
 * without 'merged column' and 'bit vector':
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [
 *        count('cat1) FILTER (WHERE (('gid = 1) AND 'max(id > 1))),
 *        count('(IF((value > 5), cat1, null))) FILTER (WHERE ('gid = 5)),
 *        count('cat2) FILTER (WHERE (('gid = 3) AND 'max(id > 2))),
 *        count('id) FILTER (WHERE ('gid = 2)),
 *        sum('value) FILTER (WHERE ('gid = 4))
 *    ]
 *    output = ['key, 'cat1_filter_cnt_dist, 'cat2_filter_cnt_dist, 'cat1_if_cnt_dist,
 *              'id_cnt_dist, 'id_sum_dist])
 *   Aggregate(
 *      key = ['key, 'cat1, 'value, 'cat2, '(IF((value > 5), cat1, null)), 'id, 'gid]
 *      functions = [max('id > 1), max('id > 2)]
 *      output = ['key, 'cat1, 'value, 'cat2, '(IF((value > 5), cat1, null)), 'id, 'gid,
 *                'max(id > 1), 'max(id > 2)])
 *     Expand(
 *        projections = [
 *          ('key, 'cat1, null, null, null, null, 1, ('id > 1), null),
 *          ('key, null, null, null, null, 'id, 2, null, null),
 *          ('key, null, null, 'cat2, null, null, 3, null, ('id > 2)),
 *          ('key, null, 'value, null, null, null, 4, null, null),
 *          ('key, null, null, null, if (('value > 5)) 'cat1 else null, null, 5, null, null)
 *        ]
 *        output = ['key, 'cat1, 'value, 'cat2, '(IF((value > 5), cat1, null)), 'id,
 *                  'gid, '(id > 1), '(id > 2)])
 *       LocalTableScan [...]
 * }}}
 *
 * This rule rewrites this logical plan to the following (pseudo) logical plan:
 * {{{
 * Aggregate(
 *    key = ['key]
 *    functions = [
 *        count('merged_string_1) FILTER (WHERE (('gid = 1) AND NOT (('filter_vector_1 & 1) = 0))),
 *        count(if (NOT (('if_vector_1 & 1) = 0)) 'merged_string_1 else null)
 *          FILTER (WHERE ('gid = 1)),
 *        count('merged_string_1) FILTER (WHERE (('gid = 2) AND NOT (('filter_vector_1 & 1) = 0))),
 *        count('merged_integer_1) FILTER (WHERE ('gid = 3)),
 *        sum('merged_integer_1) FILTER (WHERE ('gid = 4))
 *    ]
 *    output = ['key, 'cat1_filter_cnt_dist, 'cat2_filter_cnt_dist, 'cat1_if_cnt_dist,
 *              'id_cnt_dist, 'id_sum_dist])
 *   Aggregate(
 *      key = ['key, 'merged_string_1, 'merged_integer_1, 'gid]
 *      functions = [bit_or('if_vector_1),bit_or('filter_vector_1)]
 *      output = ['key, 'merged_string_1, 'merged_integer_1, 'gid,
 *        'bit_or(if_vector_1), 'bit_or(filter_vector_1)])
 *     Expand(
 *        projections = [
 *          ('key, 'cat1, null, 1, if ('value > 5) 1 else 0, if ('id > 1) 1 else 0),
 *          ('key, 'cat2, null, 2, null, if ('id > 2) 1 else 0),
 *          ('key, null, 'id, 3, null, null),
 *          ('key, null, 'value, 4, null, null)
 *        ]
 *        output = ['key, 'merged_string_1, 'merged_integer_1, 'gid,
 *          'if_vector_1, 'filter_vector_1])
 *       LocalTableScan [...]
 * }}}
 * 1. merged column: Children with same datatype from different aggregate functions
 * can share same project column (e.g. cat1, cat2).
 * 2. bit vector: If multiple aggregate function children have conditional expressions,
 * these conditions will output one column when it is true, and output null when it is false.
 * The detail logic is in [[RewriteDistinctAggregates.groupDistinctAggExpr]].
 * Then these aggregate functions can share one row group, and store the results of
 * their respective conditional expressions in the bit vector column,
 * reducing the number of rows of data expansion (e.g. cat1_filter_cnt_dist, cat1_if_cnt_dist).
 */
object RewriteDistinctAggregates extends Rule[LogicalPlan] {

  private val ZERO_LIT = Literal(0L, LongType)
  private val TRUE_LIT = Literal(true)

  private def mayNeedtoRewrite(a: Aggregate): Boolean = {
    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)
    // We need at least two distinct aggregates or the single distinct aggregate group exists filter
    // clause for this rule because aggregation strategy can handle a single distinct aggregate
    // group without filter clause.
    // This check can produce false-positives, e.g., SUM(DISTINCT a) & COUNT(DISTINCT a).
    distinctAggs.size > 1 || distinctAggs.exists(_.filter.isDefined)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE)) {
    case a: Aggregate if mayNeedtoRewrite(a) => rewrite(a)
  }

  def rewrite(a: Aggregate): Aggregate = {

    val aggExpressions = collectAggregateExprs(a)
    val distinctAggs = aggExpressions.filter(_.isDistinct)

    // Extract distinct aggregate expressions.
    val distinctAggGroups = groupDistinctAggExpr(distinctAggs)

    // Aggregation strategy can handle queries with a single distinct group without filter clause.
    if (distinctAggGroups.size > 1
      || distinctAggs.exists(_.filter.isDefined)
      || distinctAggGroups.head._2.size > 1) {
      // Create the attributes for the grouping id and the group by clause.
      val gid = AttributeReference("gid", IntegerType, nullable = false)()
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
      }
      val groupByAttrs = groupByMap.map(_._2)

      def patchAggregateFunctionChildren(
          af: AggregateFunction)(
          attrs: Expression => Option[Expression]): AggregateFunction = {
        val newChildren = af.children.map(c => attrs(c).getOrElse(c))
        af.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
      }

      // Setup distinct merged AttributeReference.
      val mergedDistinctAttrs = distinctAggGroups.keySet
        .map(_.groupBy(_.dataType).transform((_, v) => v.size))
        .reduce((m1, m2) => m1 ++ m2.map {
          case (k, v) => k -> math.max(v, m1.getOrElse(k, 0))
        }).flatMap(kv =>
        (1 to kv._2).map(id => AttributeReference("merged_" + kv._1.typeName + "_" + id, kv._1)())
      )

      // Setup vector AttributeReference.
      def setUpVectorAttr(prefix: String, size: Int):
      (immutable.IndexedSeq[AttributeReference], immutable.IndexedSeq[Alias]) = {
        (1 to math.ceil(size / java.lang.Long.SIZE.toDouble).toInt)
          .map { id =>
            val attr = AttributeReference(prefix + id, LongType)()
            (attr, Alias(BitOrAgg(attr).toAggregateExpression(), attr.name)())
          }.unzip
      }

      val filterVectorSize = distinctAggGroups.values.map(_.count(_.filter.isDefined)).max
      val distinctAggGroupsWithIfConds = distinctAggGroups.transform { (_, aggExprs) =>
        aggExprs.map { aggExpr =>
          val aggChildren = aggExpr.aggregateFunction.children
          val conds = aggChildren.flatMap(getIfCond)
          if (conds.size == aggChildren.size) (aggExpr, Some(conds))
          else (aggExpr, None)
        }
      }
      val ifVectorSize = distinctAggGroupsWithIfConds.values
        .map(_.flatMap(_._2).flatten.count(!_.eq(TRUE_LIT))).max

      val (ifVectorAttrs, ifBitAggs) = setUpVectorAttr("if_vector_", ifVectorSize)
      val (filterVectorAttrs, filterBitAggs) = setUpVectorAttr("filter_vector_", filterVectorSize)

      // Setup expand & aggregate operators for distinct aggregate expressions.
      val distinctAggOperatorMap = distinctAggGroupsWithIfConds.toSeq.zipWithIndex.map {
        case ((group, exprsAndIfConds), i) =>
          val id = Literal(i + 1)

          // Expand projection for if and filter vector AttributeReference.
          val ifBitVector: mutable.ArrayBuffer[(Expression, Expression)] =
            mutable.ArrayBuffer.empty
          val filterBitVector: mutable.ArrayBuffer[(Expression, Expression)] =
            mutable.ArrayBuffer.empty
          def fillVectorProjection(
              vectorAttrs: IndexedSeq[AttributeReference],
              bitAggs: IndexedSeq[Alias],
              bitVectorRecord: mutable.ArrayBuffer[(Expression, Expression)],
              useVector: Boolean,
              getConds: Seq[(AggregateExpression, Option[Seq[Expression]])] => Seq[Expression]) = {
            if (useVector) {
              val conds = getConds(exprsAndIfConds)
              val condAndBitMasks = conds.grouped(java.lang.Long.SIZE)
                .zipWithIndex
                .map { case (condGroup, gIdx) =>
                  condGroup.zipWithIndex.map { case (cond, idx) =>
                    val lit = Literal(1L << idx, LongType)
                    bitVectorRecord += ((bitAggs(gIdx).toAttribute, lit))
                    (cond, lit)
                  }
                }.toIterable

              vectorAttrs.zipAll(condAndBitMasks, null, null)
                .map { case (vectorAttr, condAndBitMask) =>
                  if (condAndBitMask == null) {
                    nullify(vectorAttr)
                  } else {
                    condAndBitMask.map {
                      case (ifCond, bitMask) => If(ifCond, bitMask, ZERO_LIT)
                    }.reduce(BitwiseOr)
                  }
                }
            } else {
              vectorAttrs.map(nullify(_))
            }
          }

          val useIfVector = exprsAndIfConds.exists { case (_, ifVtrOpt) =>
            ifVtrOpt.isDefined && ifVtrOpt.get.exists(!_.eq(TRUE_LIT))
          }
          val ifVectorProject = fillVectorProjection(
            ifVectorAttrs, ifBitAggs, ifBitVector, useIfVector,
            _.flatMap(_._2).flatten.filter(!_.eq(TRUE_LIT)))

          val useFilterVector = exprsAndIfConds.exists(_._1.filter.isDefined)
          val filterVectorProject = fillVectorProjection(
            filterVectorAttrs, filterBitAggs, filterBitVector, useFilterVector,
            _.flatMap(_._1.filter))

          // Expand projection for merged AttributeReference.
          val childToProjectAttr: mutable.Map[Expression, Expression] = mutable.Map.empty
          val childToProjectValue: mutable.Map[Expression, Expression] = mutable.Map.empty
          val children = if (exprsAndIfConds.size == 1) {
            // Only one expression in this group. It is safe to use its original children
            // so that they can be evaluated ahead in expand.
            mutable.Set(exprsAndIfConds.head._1.aggregateFunction.children.toArray: _*)
          } else {
            if (useIfVector || useFilterVector) {
              // Here are two conditions:
              // 1. If one child in all aggregate function is wrapped by if or case when conditions
              //   and they all return false
              // 2. If all aggregate function define filter and they all return false
              // They can be combined into three situations in project value:
              // (true/false, true): nullify all children
              // (true, false): nullify corresponding child
              // (false, false): do not change children
              val childToIfCondsOpt = if (useIfVector) {
                val childToIfConds = exprsAndIfConds.flatMap { case (aggExpr, ifCondOpt) =>
                  aggExpr.aggregateFunction.children.flatMap(getIfChild).zip(ifCondOpt.get)
                }.groupBy(_._1).map { case (child, tuples) =>
                  val ifConds = tuples.map(_._2)
                  if (ifConds.exists(_.eq(TRUE_LIT))) {
                    (child, None)
                  } else {
                    (child, Some(ifConds.reduce(Or)))
                  }
                }
                Some(childToIfConds)
              } else None

              val rowGroupFilterOpt = if (exprsAndIfConds.forall(_._1.filter.isDefined)) {
                Some(exprsAndIfConds.map(_._1.filter.get).reduce(Or))
              } else None

              if (childToIfCondsOpt.isDefined) {
                childToIfCondsOpt.get.foreach { case (child, ifCondOpt) =>
                  (ifCondOpt.isDefined, rowGroupFilterOpt.isDefined) match {
                    case (true, true) =>
                      childToProjectValue.put(child,
                        If(EqualTo(And(rowGroupFilterOpt.get, ifCondOpt.get), TRUE_LIT),
                          child, nullify(child)))
                    case (true, false) =>
                      childToProjectValue.put(child,
                        If(EqualTo(ifCondOpt.get, TRUE_LIT), child, nullify(child)))
                    case (false, true) =>
                      childToProjectValue.put(child,
                        If(EqualTo(rowGroupFilterOpt.get, TRUE_LIT), child, nullify(child)))
                    case (false, false) =>
                  }
                }
              } else if (rowGroupFilterOpt.isDefined) {
                group.foreach(child =>
                  childToProjectValue.put(child,
                    If(EqualTo(rowGroupFilterOpt.get, TRUE_LIT), child, nullify(child))))
              }
            }

            mutable.Set(group.toArray: _*)
          }
          val mergedProject = mergedDistinctAttrs.map { attr =>
            children.find(_.dataType.equals(attr.dataType)) match {
              case Some(child) =>
                childToProjectAttr.put(child, attr)
                children.remove(child)
                childToProjectValue.getOrElse(child, child)
              case None => nullify(attr)
            }
          }.toSeq :+ id

          // Final aggregate
          var ifBitPos, filterBitPos = 0
          val operators = exprsAndIfConds.map { case (expr, ifCond) =>
            val af = expr.aggregateFunction
            val useIfVector = ifCond.isDefined && ifCond.get.exists(!_.eq(TRUE_LIT))

            val naf = if (af.children.forall(_.foldable)) af
            else {
              var childIdx = 0
              patchAggregateFunctionChildren(af) { x =>
                childToProjectAttr.get(x)
                  .orElse(childToProjectAttr.get(getIfChild(x).get))
                  .map { attr =>
                    val newInput = if (useIfVector && !ifCond.get(childIdx).eq(TRUE_LIT)) {
                      val (ifVectorAttr, value) = ifBitVector(ifBitPos)
                      ifBitPos += 1
                      If(Not(EqualTo(BitwiseAnd(ifVectorAttr, value), ZERO_LIT)),
                        attr, nullify(attr))
                    }
                    else {
                      attr
                    }
                    childIdx += 1
                    newInput
                  }
              }
            }

            val newCond = if (expr.filter.isDefined) {
              val (attr, value) = filterBitVector(filterBitPos)
              filterBitPos += 1
              And(EqualTo(gid, id), Not(EqualTo(BitwiseAnd(attr, value), ZERO_LIT)))
            } else {
              EqualTo(gid, id)
            }

            (expr, expr.copy(aggregateFunction = naf, isDistinct = false, filter = Some(newCond)))
          }

          (mergedProject ++ ifVectorProject ++ filterVectorProject, operators)
      }

      // Setup expand for the 'regular' aggregate expressions.
      // only expand unfoldable children
      val regularAggExprs = aggExpressions
        .filter(e => !e.isDistinct && e.children.exists(!_.foldable))
      val regularAggFunChildren = regularAggExprs
        .flatMap(_.aggregateFunction.children.filter(!_.foldable))
      val regularAggFilterAttrs = regularAggExprs.flatMap(_.filterAttributes)
      val regularAggChildren = (regularAggFunChildren ++ regularAggFilterAttrs).distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair)

      // Setup aggregates for 'regular' aggregate expressions.
      val regularGroupId = Literal(0)
      val regularAggChildAttrLookup = regularAggChildAttrMap.toMap
      val regularAggOperatorMap = regularAggExprs.map { e =>
        // Perform the actual aggregation in the initial aggregate.
        val af = patchAggregateFunctionChildren(e.aggregateFunction)(regularAggChildAttrLookup.get)
        // We changed the attributes in the [[Expand]] output using expressionAttributePair.
        // So we need to replace the attributes in FILTER expression with new ones.
        val filterOpt = e.filter.map(_.transform {
          case a: Attribute => regularAggChildAttrLookup.getOrElse(a, a)
        })
        val operator = Alias(e.copy(aggregateFunction = af, filter = filterOpt), e.sql)()

        // Select the result of the first aggregate in the last aggregate.
        val result = AggregateExpression(
          aggregate.First(operator.toAttribute, ignoreNulls = true),
          mode = Complete,
          isDistinct = false,
          filter = Some(EqualTo(gid, regularGroupId)))

        // Some aggregate functions (COUNT) have the special property that they can return a
        // non-null result without any input. We need to make sure we return a result in this case.
        val resultWithDefault = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(result, lit))
          case None => result
        }

        // Return a Tuple3 containing:
        // i. The original aggregate expression (used for look ups).
        // ii. The actual aggregation operator (used in the first aggregate).
        // iii. The operator that selects and returns the result (used in the second aggregate).
        (e, operator, resultWithDefault)
      }

      // Construct the regular aggregate input projection only if we need one.
      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          mergedDistinctAttrs.map(nullify) ++
          Seq(regularGroupId) ++
          ifVectorAttrs.map(nullify) ++
          filterVectorAttrs.map(nullify) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }

      // Construct the distinct aggregate input projections.
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjections = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      // Construct the expand operator.
      val expand = Expand(
        regularAggProjection ++ distinctAggProjections,
        groupByAttrs ++ mergedDistinctAttrs ++ Seq(gid) ++
          ifVectorAttrs ++ filterVectorAttrs ++
          regularAggChildAttrMap.map(_._2),
        a.child)

      // Construct the first aggregate operator. This de-duplicates all the children of
      // distinct operators, and applies the regular aggregate operators.
      val firstAggregateGroupBy = groupByAttrs ++ mergedDistinctAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ ifBitAggs ++ filterBitAggs ++ regularAggOperatorMap.map(_._2),
        expand)

      // Construct the second aggregate
      val transformations: Map[Expression, Expression] =
        (distinctAggOperatorMap.flatMap(_._2) ++
          regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            // The same GROUP BY clauses can have different forms (different names for instance) in
            // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
            // tricky. So we do a linear search for a semantically equal group by expression.
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    } else {
      a
    }
  }

  def getIfChild(e: Expression): Option[Expression] = {
    e match {
      case CaseWhen(Seq((_, thenValue: LeafExpression)), None | Some(Literal(null, _)))
      => Some(thenValue)
      case If(_, ifValue: LeafExpression, Literal(null, _)) => Some(ifValue)
      case e: LeafExpression => Some(e)
      case _: Expression => None
    }
  }

  def getIfCond(e: Expression): Option[Expression] = {
    e match {
      case CaseWhen(Seq((cond, _: LeafExpression)), None | Some(Literal(null, _))) => Some(cond)
      case If(cond, _: LeafExpression, Literal(null, _)) => Some(cond)
      case _: LeafExpression => Some(TRUE_LIT)
      case _: Expression => None
    }
  }

  def groupDistinctAggExpr(aggExpressions: Seq[AggregateExpression]):
  Map[Set[Expression], Seq[AggregateExpression]] = {
    val distinctAggGroups = aggExpressions.filter(_.isDistinct).groupBy { e =>
      val unfoldableChildren = e.aggregateFunction.children.filter(!_.foldable).toSet
      if (unfoldableChildren.nonEmpty) {
        // Only expand the unfoldable children
        val bitVectorizableChildren = unfoldableChildren.flatMap(getIfChild)
        if (bitVectorizableChildren.size == unfoldableChildren.size) bitVectorizableChildren
        else unfoldableChildren
      } else {
        // If aggregateFunction's children are all foldable
        // we must expand at least one of the children (here we take the first child),
        // or If we don't, we will get the wrong result, for example:
        // count(distinct 1) will be explained to count(1) after the rewrite function.
        // Generally, the distinct aggregateFunction should not run
        // foldable TypeCheck for the first child.
        e.aggregateFunction.children.take(1).toSet
      }
    }
    distinctAggGroups
  }

  private def collectAggregateExprs(a: Aggregate): Seq[AggregateExpression] = {
    // Collect all aggregate expressions.
    a.aggregateExpressions.flatMap { _.collect {
        case ae: AggregateExpression => ae
    }}
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  private def expressionAttributePair(e: Expression) =
    // We are creating a new reference here instead of reusing the attribute in case of a
    // NamedExpression. This is done to prevent collisions between distinct and regular aggregate
    // children, in this case attribute reuse causes the input of the regular aggregate to bound to
    // the (nulled out) input of the distinct aggregate.
    e -> AttributeReference(e.sql, e.dataType, nullable = true)()
}
