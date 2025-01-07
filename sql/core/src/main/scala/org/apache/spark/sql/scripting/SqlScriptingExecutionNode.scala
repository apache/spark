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

package org.apache.spark.sql.scripting

import java.util

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{NameParameterizedQuery, UnresolvedAttribute, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateArray, CreateMap, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DefaultValueExpression, DropVariable, LogicalPlan, OneRowRelation, Project, SetVariable}
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}
import org.apache.spark.sql.errors.SqlScriptingErrors
import org.apache.spark.sql.types.BooleanType

/**
 * Trait for all SQL scripting execution nodes used during interpretation phase.
 */
sealed trait CompoundStatementExec extends Logging {

  /**
   * Whether the statement originates from the SQL script or is created during the interpretation.
   * Example: DropVariable statements are automatically created at the end of each compound.
   */
  val isInternal: Boolean = false

  /**
   * Reset execution of the current node.
   */
  def reset(): Unit
}

/**
 * Leaf node in the execution tree.
 */
trait LeafStatementExec extends CompoundStatementExec

/**
 * Non-leaf node in the execution tree. It is an iterator over executable child nodes.
 */
trait NonLeafStatementExec extends CompoundStatementExec {

  /**
   * Construct the iterator to traverse the tree rooted at this node in an in-order traversal.
   * @return
   *   Tree iterator.
   */
  def getTreeIterator: Iterator[CompoundStatementExec]

  /**
   * Evaluate the boolean condition represented by the statement.
   * @param session SparkSession that SQL script is executed within.
   * @param statement Statement representing the boolean condition to evaluate.
   * @return
   *    The value (`true` or `false`) of condition evaluation;
   *    or throw the error during the evaluation (eg: returning multiple rows of data
   *    or non-boolean statement).
   */
  protected def evaluateBooleanCondition(
      session: SparkSession,
      statement: LeafStatementExec): Boolean = statement match {
    case statement: SingleStatementExec =>
      assert(!statement.isExecuted)
      statement.isExecuted = true

      // DataFrame evaluates to True if it is single row, single column
      //  of boolean type with value True.
      val df = statement.buildDataFrame(session)
      df.schema.fields match {
        case Array(field) if field.dataType == BooleanType =>
          df.limit(2).collect() match {
            case Array(row) =>
              if (row.isNullAt(0)) {
                throw SqlScriptingErrors.booleanStatementWithEmptyRow(
                  statement.origin, statement.getText)
              }
              row.getBoolean(0)
            case _ =>
              throw SparkException.internalError(
                s"Boolean statement ${statement.getText} is invalid. It returns more than one row.")
          }
        case _ =>
          throw SqlScriptingErrors.invalidBooleanStatement(statement.origin, statement.getText)
      }
    case _ =>
      throw SparkException.internalError("Boolean condition must be SingleStatementExec")
  }
}

/**
 * Executable node for SingleStatement.
 * @param parsedPlan
 *   Logical plan of the parsed statement.
 * @param origin
 *   Origin descriptor for the statement.
 * @param args
 *   A map of parameter names to SQL literal expressions.
 * @param isInternal
 *   Whether the statement originates from the SQL script or it is created during the
 *   interpretation. Example: DropVariable statements are automatically created at the end of each
 *   compound.
 * @param context
 *   SqlScriptingExecutionContext keeps the execution state of current script.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    val args: Map[String, Expression],
    override val isInternal: Boolean,
    context: SqlScriptingExecutionContext)
  extends LeafStatementExec with WithOrigin {

  /**
   * Whether this statement has been executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var isExecuted = false

  /**
   * Plan with named parameters.
   */
  private lazy val preparedPlan: LogicalPlan = {
    if (args.nonEmpty) {
      NameParameterizedQuery(parsedPlan, args)
    } else {
      parsedPlan
    }
  }

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }

  /**
   * Builds a DataFrame from the parsedPlan of this SingleStatementExec
   * @param session The SparkSession on which the parsedPlan is built.
   * @return
   *   The DataFrame.
   */
  def buildDataFrame(session: SparkSession): DataFrame = {
    Dataset.ofRows(session, preparedPlan)
  }

  override def reset(): Unit = isExecuted = false
}

/**
 * NO-OP leaf node, which does nothing when returned to the iterator.
 * It is emitted by empty BEGIN END blocks.
 */
class NoOpStatementExec extends LeafStatementExec {
  override def reset(): Unit = ()
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 * @param label
 *   Label set by user to CompoundBody or None otherwise.
 * @param isScope
 *   Flag that indicates whether Compound Body is scope or not.
 * @param context
 *   SqlScriptingExecutionContext keeps the execution state of current script.
 */
class CompoundBodyExec(
    statements: Seq[CompoundStatementExec],
    label: Option[String] = None,
    isScope: Boolean,
    context: SqlScriptingExecutionContext)
  extends NonLeafStatementExec {

  private object ScopeStatus extends Enumeration {
    type ScopeStatus = Value
    val NOT_ENTERED, INSIDE, EXITED = Value
  }

  private var localIterator = statements.iterator
  private var curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  private var scopeStatus = ScopeStatus.NOT_ENTERED

  /**
   * Enter scope represented by this compound statement.
   *
   * This operation needs to be idempotent because it is called multiple times during
   * iteration, but it should be executed only once when compound body that represent
   * scope is encountered for the first time.
   */
  def enterScope(): Unit = {
    // This check makes this operation idempotent.
    if (isScope && scopeStatus == ScopeStatus.NOT_ENTERED) {
      scopeStatus = ScopeStatus.INSIDE
      context.enterScope(label.get)
    }
  }

  /**
   * Exit scope represented by this compound statement.
   *
   * Even though this operation is called exactly once, we are making it idempotent.
   */
  protected def exitScope(): Unit = {
    // This check makes this operation idempotent.
    if (isScope && scopeStatus == ScopeStatus.INSIDE) {
      scopeStatus = ScopeStatus.EXITED
      context.exitScope(label.get)
    }
  }

  /** Used to stop the iteration in cases when LEAVE statement is encountered. */
  private var stopIteration = false

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = {
        val childHasNext = curr match {
          case Some(body: NonLeafStatementExec) => body.getTreeIterator.hasNext
          case Some(_: LeafStatementExec) => true
          case None => false
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
        !stopIteration && (localIterator.hasNext || childHasNext)
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(leaveStatement: LeaveStatementExec) =>
            handleLeaveStatement(leaveStatement)
            curr = None
            leaveStatement
          case Some(iterateStatement: IterateStatementExec) =>
            handleIterateStatement(iterateStatement)
            curr = None
            iterateStatement
          case Some(statement: LeafStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body match {
                // Scope will be entered only once per compound because enter scope is idempotent.
                case compoundBodyExec: CompoundBodyExec => compoundBodyExec.enterScope()
                case _ => // pass
              }
              body.getTreeIterator.next() match {
                case leaveStatement: LeaveStatementExec =>
                  handleLeaveStatement(leaveStatement)
                  leaveStatement
                case iterateStatement: IterateStatementExec =>
                  handleIterateStatement(iterateStatement)
                  iterateStatement
                case other => other
              }
            } else {
              body match {
                // Exit scope when there are no more statements to iterate through.
                case compoundBodyExec: CompoundBodyExec => compoundBodyExec.exitScope()
                case _ => // pass
              }
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    statements.foreach(_.reset())
    localIterator = statements.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
    stopIteration = false
    scopeStatus = ScopeStatus.NOT_ENTERED
  }

  /** Actions to do when LEAVE statement is encountered, to stop the execution of this compound. */
  private def handleLeaveStatement(leaveStatement: LeaveStatementExec): Unit = {
    if (!leaveStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // Exit scope if leave statement is encountered.
      exitScope()

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // Check if label has been matched.
      leaveStatement.hasBeenMatched = label.isDefined && label.get.equals(leaveStatement.label)
    }
  }

  /**
   * Actions to do when ITERATE statement is encountered, to stop the execution of this compound.
   */
  private def handleIterateStatement(iterateStatement: IterateStatementExec): Unit = {
    if (!iterateStatement.hasBeenMatched) {
      // Stop the iteration.
      stopIteration = true

      // Exit scope if iterate statement is encountered.
      exitScope()

      // TODO: Variable cleanup (once we add SQL script execution logic).
      // TODO: Add interpreter tests as well.

      // No need to check if label has been matched, since ITERATE statement is already
      //   not allowed in CompoundBody.
    }
  }
}

/**
 * Executable node for IfElseStatement.
 * @param conditions Collection of executable conditions. First condition corresponds to IF clause,
 *                   while others (if any) correspond to following ELSE IF clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
*                 in IF or ELSE IF branches.
 * @param elseBody Body that is executed if none of the conditions are met,
 *                          i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 */
class IfElseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object IfElseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = IfElseState.Condition
  private var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length
  assert(conditionsCount == conditionalBodies.length)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case IfElseState.Condition =>
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (evaluateBooleanCondition(session, condition)) {
            state = IfElseState.Body
            curr = Some(conditionalBodies(clauseIdx))
          } else {
            clauseIdx += 1
            if (clauseIdx < conditionsCount) {
              // There are ELSE IF clauses remaining.
              state = IfElseState.Condition
              curr = Some(conditions(clauseIdx))
            } else if (elseBody.isDefined) {
              // ELSE clause exists.
              state = IfElseState.Body
              curr = Some(elseBody.get)
            } else {
              // No remaining clauses.
              curr = None
            }
          }
          condition
        case IfElseState.Body =>
          assert(curr.get.isInstanceOf[CompoundBodyExec])
          val currBody = curr.get.asInstanceOf[CompoundBodyExec]
          val retStmt = currBody.getTreeIterator.next()
          if (!currBody.getTreeIterator.hasNext) {
            curr = None
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = IfElseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for WhileStatement.
 * @param condition Executable node for the condition.
 * @param body Executable node for the body.
 * @param label Label set to WhileStatement by user or None otherwise.
 * @param session Spark session that SQL script is executed within.
 */
class WhileStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

  private object WhileState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = WhileState.Condition
  private var curr: Option[CompoundStatementExec] = Some(condition)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
          case WhileState.Condition =>
            val condition = curr.get.asInstanceOf[SingleStatementExec]
            if (evaluateBooleanCondition(session, condition)) {
              state = WhileState.Body
              curr = Some(body)
              body.reset()
            } else {
              curr = None
            }
            condition
          case WhileState.Body =>
            val retStmt = body.getTreeIterator.next()

            // Handle LEAVE or ITERATE statement if it has been encountered.
            retStmt match {
              case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
                if (label.contains(leaveStatementExec.label)) {
                  leaveStatementExec.hasBeenMatched = true
                }
                curr = None
                return retStmt
              case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
                if (label.contains(iterStatementExec.label)) {
                  iterStatementExec.hasBeenMatched = true
                }
                state = WhileState.Condition
                curr = Some(condition)
                condition.reset()
                return retStmt
              case _ =>
            }

            if (!body.getTreeIterator.hasNext) {
              state = WhileState.Condition
              curr = Some(condition)
              condition.reset()
            }
            retStmt
        }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = WhileState.Condition
    curr = Some(condition)
    condition.reset()
    body.reset()
  }
}

/**
 * Executable node for CaseStatement.
 * @param conditions Collection of executable conditions which correspond to WHEN clauses.
 * @param conditionalBodies Collection of executable bodies that have a corresponding condition,
 *                 in WHEN branches.
 * @param elseBody Body that is executed if none of the conditions are met, i.e. ELSE branch.
 * @param session Spark session that SQL script is executed within.
 */
class CaseStatementExec(
    conditions: Seq[SingleStatementExec],
    conditionalBodies: Seq[CompoundBodyExec],
    elseBody: Option[CompoundBodyExec],
    session: SparkSession) extends NonLeafStatementExec {
  private object CaseState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = CaseState.Condition
  private var curr: Option[CompoundStatementExec] = Some(conditions.head)

  private var clauseIdx: Int = 0
  private val conditionsCount = conditions.length

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case CaseState.Condition =>
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (evaluateBooleanCondition(session, condition)) {
            state = CaseState.Body
            curr = Some(conditionalBodies(clauseIdx))
          } else {
            clauseIdx += 1
            if (clauseIdx < conditionsCount) {
              // There are WHEN clauses remaining.
              state = CaseState.Condition
              curr = Some(conditions(clauseIdx))
            } else if (elseBody.isDefined) {
              // ELSE clause exists.
              state = CaseState.Body
              curr = Some(elseBody.get)
            } else {
              // No remaining clauses.
              curr = None
            }
          }
          condition
        case CaseState.Body =>
          assert(curr.get.isInstanceOf[CompoundBodyExec])
          val currBody = curr.get.asInstanceOf[CompoundBodyExec]
          val retStmt = currBody.getTreeIterator.next()
          if (!currBody.getTreeIterator.hasNext) {
            curr = None
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = CaseState.Condition
    curr = Some(conditions.head)
    clauseIdx = 0
    conditions.foreach(c => c.reset())
    conditionalBodies.foreach(b => b.reset())
    elseBody.foreach(b => b.reset())
  }
}

/**
 * Executable node for RepeatStatement.
 * @param condition Executable node for the condition - evaluates to a row with a single boolean
 *                  expression, otherwise throws an exception
 * @param body Executable node for the body.
 * @param label Label set to RepeatStatement by user, None if not set
 * @param session Spark session that SQL script is executed within.
 */
class RepeatStatementExec(
    condition: SingleStatementExec,
    body: CompoundBodyExec,
    label: Option[String],
    session: SparkSession) extends NonLeafStatementExec {

  private object RepeatState extends Enumeration {
    val Condition, Body = Value
  }

  private var state = RepeatState.Body
  private var curr: Option[CompoundStatementExec] = Some(body)

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = curr.nonEmpty

      override def next(): CompoundStatementExec = state match {
        case RepeatState.Condition =>
          val condition = curr.get.asInstanceOf[SingleStatementExec]
          if (!evaluateBooleanCondition(session, condition)) {
            state = RepeatState.Body
            curr = Some(body)
            body.reset()
          } else {
            curr = None
          }
          condition
        case RepeatState.Body =>
          val retStmt = body.getTreeIterator.next()

          retStmt match {
            case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
              if (label.contains(leaveStatementExec.label)) {
                leaveStatementExec.hasBeenMatched = true
              }
              curr = None
              return retStmt
            case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
              if (label.contains(iterStatementExec.label)) {
                iterStatementExec.hasBeenMatched = true
              }
              state = RepeatState.Condition
              curr = Some(condition)
              condition.reset()
              return retStmt
            case _ =>
          }

          if (!body.getTreeIterator.hasNext) {
            state = RepeatState.Condition
            curr = Some(condition)
            condition.reset()
          }
          retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = RepeatState.Body
    curr = Some(body)
    body.reset()
    condition.reset()
  }
}

/**
 * Executable node for LeaveStatement.
 * @param label Label of the compound or loop to leave.
 */
class LeaveStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the LEAVE statement might not belong to the immediate surrounding compound,
   *   but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   *   compound is encountered, next() will be called to iterate its body. The same logic
   *   is applied to any other compound down the traversal tree.
   * In such cases, when LEAVE statement is encountered (as the leaf of the traversal tree),
   *   it will be propagated upwards and the logic will try to match it to the labels of
   *   surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}

/**
 * Executable node for ITERATE statement.
 * @param label Label of the loop to iterate.
 */
class IterateStatementExec(val label: String) extends LeafStatementExec {
  /**
   * Label specified in the ITERATE statement might not belong to the immediate compound,
   * but to the any surrounding compound.
   * Iteration logic is recursive, i.e. when iterating through the compound, if another
   * compound is encountered, next() will be called to iterate its body. The same logic
   * is applied to any other compound down the tree.
   * In such cases, when ITERATE statement is encountered (as the leaf of the traversal tree),
   * it will be propagated upwards and the logic will try to match it to the labels of
   * surrounding compounds.
   * Once the match is found, this flag is set to true to indicate that search should be stopped.
   */
  var hasBeenMatched: Boolean = false
  override def reset(): Unit = hasBeenMatched = false
}

/**
 * Executable node for LoopStatement.
 * @param body Executable node for the body, executed on every loop iteration.
 * @param label Label set to LoopStatement by user, None if not set.
 */
class LoopStatementExec(
    body: CompoundBodyExec,
    val label: Option[String]) extends NonLeafStatementExec {

  /**
   * Loop can be interrupted by LeaveStatementExec
   */
  private var interrupted: Boolean = false

  /**
   * Loop can be iterated by IterateStatementExec
   */
  private var iterated: Boolean = false

  private lazy val treeIterator =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = !interrupted

      override def next(): CompoundStatementExec = {
        if (!body.getTreeIterator.hasNext || iterated) {
          reset()
        }

        val retStmt = body.getTreeIterator.next()

        retStmt match {
          case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
            if (label.contains(leaveStatementExec.label)) {
              leaveStatementExec.hasBeenMatched = true
            }
            interrupted = true
          case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
            if (label.contains(iterStatementExec.label)) {
              iterStatementExec.hasBeenMatched = true
            }
            iterated = true
          case _ =>
        }

        retStmt
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    interrupted = false
    iterated = false
    body.reset()
  }
}

/**
 * Executable node for ForStatement.
 * @param query Executable node for the query.
 * @param variableName Name of variable used for accessing current row during iteration.
 * @param body Executable node for the body.
 * @param label Label set to ForStatement by user or None otherwise.
 * @param session Spark session that SQL script is executed within.
 * @param context SqlScriptingExecutionContext keeps the execution state of current script.
 */
class ForStatementExec(
    query: SingleStatementExec,
    variableName: Option[String],
    body: CompoundBodyExec,
    val label: Option[String],
    session: SparkSession,
    context: SqlScriptingExecutionContext) extends NonLeafStatementExec {

  private object ForState extends Enumeration {
    val VariableAssignment, Body, VariableCleanup = Value
  }
  private var state = ForState.VariableAssignment
  private var areVariablesDeclared = false

  // map of all variables created internally by the for statement
  // (variableName -> variableExpression)
  private var variablesMap: Map[String, Expression] = Map()

  // compound body used for dropping variables while in ForState.VariableAssignment
  private var dropVariablesExec: CompoundBodyExec = null

  private var queryResult: util.Iterator[Row] = _
  private var isResultCacheValid = false
  private def cachedQueryResult(): util.Iterator[Row] = {
    if (!isResultCacheValid) {
      queryResult = query.buildDataFrame(session).toLocalIterator()
      query.isExecuted = true
      isResultCacheValid = true
    }
    queryResult
  }

  /**
   * For can be interrupted by LeaveStatementExec
   */
  private var interrupted: Boolean = false

  private lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {

      override def hasNext: Boolean = !interrupted && (state match {
          case ForState.VariableAssignment => cachedQueryResult().hasNext
          case ForState.Body => true
          case ForState.VariableCleanup => dropVariablesExec.getTreeIterator.hasNext
        })

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = state match {

        case ForState.VariableAssignment =>
          variablesMap = createVariablesMapFromRow(cachedQueryResult().next())

          if (!areVariablesDeclared) {
            // create and execute declare var statements
            variablesMap.keys.toSeq
              .map(colName => createDeclareVarExec(colName, variablesMap(colName)))
              .foreach(declareVarExec => declareVarExec.buildDataFrame(session).collect())
            areVariablesDeclared = true
          }

          // create and execute set var statements
          variablesMap.keys.toSeq
            .map(colName => createSetVarExec(colName, variablesMap(colName)))
            .foreach(setVarExec => setVarExec.buildDataFrame(session).collect())

          state = ForState.Body
          body.reset()
          next()

        case ForState.Body =>
          val retStmt = body.getTreeIterator.next()

          // Handle LEAVE or ITERATE statement if it has been encountered.
          retStmt match {
            case leaveStatementExec: LeaveStatementExec if !leaveStatementExec.hasBeenMatched =>
              if (label.contains(leaveStatementExec.label)) {
                leaveStatementExec.hasBeenMatched = true
              }
              interrupted = true
              // If this for statement encounters LEAVE, it will either not be executed
              // again, or it will be reset before being executed.
              // In either case, variables will not
              // be dropped normally, from ForState.VariableCleanup, so we drop them here.
              dropVars()
              return retStmt
            case iterStatementExec: IterateStatementExec if !iterStatementExec.hasBeenMatched =>
              if (label.contains(iterStatementExec.label)) {
                iterStatementExec.hasBeenMatched = true
              } else {
                // if an outer loop is being iterated, this for statement will either not be
                // executed again, or it will be reset before being executed.
                // In either case, variables will not
                // be dropped normally, from ForState.VariableCleanup, so we drop them here.
                dropVars()
              }
              switchStateFromBody()
              return retStmt
            case _ =>
          }

          if (!body.getTreeIterator.hasNext) {
            switchStateFromBody()
          }
          retStmt

        case ForState.VariableCleanup =>
          dropVariablesExec.getTreeIterator.next()
      }
    }

  /**
   * Recursively creates a Catalyst expression from Scala value.<br>
   * See https://spark.apache.org/docs/latest/sql-ref-datatypes.html for Spark -> Scala mappings
   */
  private def createExpressionFromValue(value: Any): Expression = value match {
    case m: Map[_, _] =>
      // arguments of CreateMap are in the format: (key1, val1, key2, val2, ...)
      val mapArgs = m.keys.toSeq.flatMap { key =>
        Seq(createExpressionFromValue(key), createExpressionFromValue(m(key)))
      }
      CreateMap(mapArgs, useStringTypeWhenEmpty = false)

    // structs and rows match this case
    case s: Row =>
    // arguments of CreateNamedStruct are in the format: (name1, val1, name2, val2, ...)
    val namedStructArgs = s.schema.names.toSeq.flatMap { colName =>
        val valueExpression = createExpressionFromValue(s.getAs(colName))
        Seq(Literal(colName), valueExpression)
      }
      CreateNamedStruct(namedStructArgs)

    // arrays match this case
    case a: collection.Seq[_] =>
      val arrayArgs = a.toSeq.map(createExpressionFromValue(_))
      CreateArray(arrayArgs, useStringTypeWhenEmpty = false)

    case _ => Literal(value)
  }

  private def createVariablesMapFromRow(row: Row): Map[String, Expression] = {
    var variablesMap = row.schema.names.toSeq.map { colName =>
      colName -> createExpressionFromValue(row.getAs(colName))
    }.toMap

    if (variableName.isDefined) {
      val namedStructArgs = variablesMap.keys.toSeq.flatMap { colName =>
        Seq(Literal(colName), variablesMap(colName))
      }
      val forVariable = CreateNamedStruct(namedStructArgs)
      variablesMap = variablesMap + (variableName.get -> forVariable)
    }
    variablesMap
  }

  /**
   * Create and immediately execute dropVariable exec nodes for all variables in variablesMap.
   */
  private def dropVars(): Unit = {
    variablesMap.keys.toSeq
      .map(colName => createDropVarExec(colName))
      .foreach(dropVarExec => dropVarExec.buildDataFrame(session).collect())
    areVariablesDeclared = false
  }

  private def switchStateFromBody(): Unit = {
    state = if (cachedQueryResult().hasNext) ForState.VariableAssignment
    else {
      // create compound body for dropping nodes after execution is complete
      dropVariablesExec = new CompoundBodyExec(
        variablesMap.keys.toSeq.map(colName => createDropVarExec(colName)),
        None,
        isScope = false,
        context
      )
      ForState.VariableCleanup
    }
  }

  private def createDeclareVarExec(varName: String, variable: Expression): SingleStatementExec = {
    val defaultExpression = DefaultValueExpression(Literal(null, variable.dataType), "null")
    val declareVariable = CreateVariable(
      UnresolvedIdentifier(Seq(varName)),
      defaultExpression,
      replace = true
    )
    new SingleStatementExec(declareVariable, Origin(), Map.empty, isInternal = true, context)
  }

  private def createSetVarExec(varName: String, variable: Expression): SingleStatementExec = {
    val projectNamedStruct = Project(
      Seq(Alias(variable, varName)()),
      OneRowRelation()
    )
    val setIdentifierToCurrentRow =
      SetVariable(Seq(UnresolvedAttribute(varName)), projectNamedStruct)
    new SingleStatementExec(
      setIdentifierToCurrentRow,
      Origin(),
      Map.empty,
      isInternal = true,
      context)
  }

  private def createDropVarExec(varName: String): SingleStatementExec = {
    val dropVar = DropVariable(UnresolvedIdentifier(Seq(varName)), ifExists = true)
    new SingleStatementExec(dropVar, Origin(), Map.empty, isInternal = true, context)
  }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    state = ForState.VariableAssignment
    isResultCacheValid = false
    variablesMap = Map()
    areVariablesDeclared = false
    dropVariablesExec = null
    interrupted = false
    body.reset()
  }
}
