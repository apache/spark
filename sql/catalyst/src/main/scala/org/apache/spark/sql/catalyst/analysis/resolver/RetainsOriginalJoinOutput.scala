package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{HashMap, HashSet}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util._

trait RetainsOriginalJoinOutput {

  /**
   * This method adds a [[Project]] node on top of a [[Join]], if [[Join]]'s output has been
   * changed when metadata columns are added to [[Project]] nodes below the [[Join]]. This is
   * necessary in order to stay compatible with fixed-point analyzer. Instead of doing this in
   * [[JoinResolver]] we must do this here, because while resolving [[Join]] we still don't know if
   * we should add a [[Project]] or not. For example consider the following query:
   *
   * {{{
   * -- tables: nt1(k, v1), nt2(k, v2), nt3(k, v3)
   * SELECT * FROM nt1 NATURAL JOIN nt2 JOIN nt3 ON nt2.k = nt3.k;
   * }}}
   *
   * Unresolved plan will be:
   *
   * 'Project [*]
   * +- 'Join Inner, ('nt2.k = 'nt3.k)
   * :- 'Join NaturalJoin(Inner)
   * :  :- 'UnresolvedRelation [nt1], [], false
   * :  +- 'UnresolvedRelation [nt2], [], false
   * +- 'UnresolvedRelation [nt3], [], false
   *
   * After resolving the inner natural join, the plan becomes:
   *
   * 'Project [*]
   * +- 'Join Inner, ('nt2.k = 'nt3.k)
   *    :- Project [k#15, v1#16, v2#28, k#27]
   *    :  +- Join Inner, (k#15 = k#27)
   *    :  :  +- SubqueryAlias nt1
   *    :  :     +- LocalRelation [k#15, v1#16]
   *    :  +- SubqueryAlias nt2
   *    :     +- LocalRelation [k#27, v2#28]
   *    +- 'UnresolvedRelation [nt3], [], false
   *
   * Because we are resolving a natural join, we have placed a [[Project]] node on top of it with
   * the inner join's output. Additionally, in single-pass, we add all metadata columns as we
   * resolve up and then prune away unnecessary columns later (more in [[PruneMetadataColumns]]).
   * This is necessary in order to stay compatible with fixed-point's [[AddMetadataColumns]] rule,
   * because [[AddMetadata]] columns will recognize k#27 as missing attribute needed for [[Join]]
   * condition and will therefore add it in the below [[Project]] node. Because of this we are also
   * adding k#27 as a metadata column to this [[Project]]. This addition of a metadata column
   * changes the original output of the outer join (because one of the inputs has changed) and in
   * order to stay compatible with fixed-point, we need to place another [[Project]] on top of the
   * outer join with its original output. Now, the final plan looks like this:
   *
   * Project [k#15, v1#16, v2#28, k#31, v3#32]
   * +- Project [k#15, v1#16, v2#28, k#31, v3#32]
   *    +- Join Inner, (k#27 = k#31)
   *    :- Project [k#15, v1#16, v2#28, k#27]
   *    :  +- Join Inner, (k#15 = k#27)
   *    :     :- SubqueryAlias nt1
   *    :     :  +- LocalRelation [k#15, v1#16]
   *    :     +- SubqueryAlias nt2
   *    :        +- LocalRelation [k#27, v2#28]
   *    +- SubqueryAlias nt3
   *        +- LocalRelation [k#31, v3#32]
   *
   * As can be seen, the [[Project]] node immediately on top of [[Join]] doesn't contain the
   * metadata column k#27 that we have added. Because of this, k#27 will be pruned away later.
   *
   * Now consider the following query for the same input:
   *
   * {{{ SELECT *, nt2.k FROM nt1 NATURAL JOIN nt2 JOIN nt3 ON nt2.k = nt3.k; }}}
   *
   * The plan will be:
   *
   * Project [k#15, v1#16, v2#28, k#31, v3#32, k#27]
   * +- Join Inner, (k#27 = k#31)
   * :- Project [k#15, v1#16, v2#28, k#27]
   * :  +- Join Inner, (k#15 = k#27)
   * :     :- SubqueryAlias nt1
   * :     :  +- LocalRelation [k#15, v1#16]
   * :     +- SubqueryAlias nt2
   * :        +- LocalRelation [k#27, v2#28]
   * +- SubqueryAlias nt3
   *    +- LocalRelation [k#31, v3#32]
   *
   * In fixed-point, because we are referencing k#27 from [[Project]] node, [[AddMetadataColumns]]
   * (which is transforming the tree top-down) will see that [[Project]] has a missing metadata
   * column and will therefore place k#27 in the [[Project]] node below outer [[Join]]. This is
   * important, because by [[AddMetadataColumns]] logic, we don't check whether the output of the
   * outer [[Join]] has changed, and we only check the output change for top-most [[Project]].
   * Because we need to stay fully compatible with fixed-point, in this case w don't place a
   * [[Project]] on top of the outer [[Join]] even though its output has changed.
   *
   * The above example also holds true for cases when there is a [[Project]], [[Aggregate]] or
   * [[Filter]] node on top of a [[Join]].
   */
  def retainOriginalJoinOutput(
      plan: LogicalPlan,
      outputExpressions: Seq[NamedExpression],
      scopes: NameScopeStack,
      childReferencedAttributes: HashMap[ExprId, Attribute]): LogicalPlan = {
    plan match {
      case join: Join
          if childHasMissingAttributesNotInOutput(
            scopes = scopes,
            outputExpressions = outputExpressions,
            referencedAttributes = childReferencedAttributes
          ) =>
        Project(scopes.current.output, join)
      case other => other
    }
  }

  /**
   * Returns true if a node has missing attributes that can be resolved from
   * [[NameScope.hiddenOutput]] and those attributes are not present in the output.
   */
  private def childHasMissingAttributesNotInOutput(
      scopes: NameScopeStack,
      outputExpressions: Seq[NamedExpression],
      referencedAttributes: HashMap[ExprId, Attribute]): Boolean = {
    val expressionIdsFromOutput = new HashSet[ExprId](outputExpressions.map(_.exprId).asJava)
    val missingAttributes = new HashMap[ExprId, Attribute]
    referencedAttributes.asScala
      .foreach {
        case (exprId, attribute) =>
          if (!expressionIdsFromOutput.contains(exprId) && attribute.isMetadataCol) {
            missingAttributes.put(exprId, attribute)
          }
      }
    val missingAttributeResolvedByHiddenOutput =
      scopes.current.resolveMissingAttributesByHiddenOutput(missingAttributes)

    missingAttributeResolvedByHiddenOutput.nonEmpty
  }
}
