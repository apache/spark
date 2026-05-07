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

package org.apache.spark.sql.catalyst.plans

import java.util.Locale

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Attribute

object JoinType {

  val supported = Seq(
    "inner",
    "outer", "full", "fullouter", "full_outer",
    "leftouter", "left", "left_outer",
    "rightouter", "right", "right_outer",
    "leftsemi", "left_semi", "semi",
    "leftanti", "left_anti", "anti",
    "cross"
  )

  def apply(typ: String): JoinType = typ.toLowerCase(Locale.ROOT).replace("_", "") match {
    case "inner" => Inner
    case "outer" | "full" | "fullouter" => FullOuter
    case "leftouter" | "left" => LeftOuter
    case "rightouter" | "right" => RightOuter
    case "leftsemi" | "semi" => LeftSemi
    case "leftanti" | "anti" => LeftAnti
    case "cross" => Cross
    case _ =>
      throw new AnalysisException(
        errorClass = "UNSUPPORTED_JOIN_TYPE",
        messageParameters = Map(
          "typ" -> typ,
          "supported" -> supported.mkString("'", "', '", "'"))
      )
  }
}

sealed abstract class JoinType {
  def sql: String
}

/**
 * The explicitCartesian flag indicates if the inner join was constructed with a CROSS join
 * indicating a cartesian product has been explicitly requested.
 */
sealed abstract class InnerLike extends JoinType {
  def explicitCartesian: Boolean
}

case object Inner extends InnerLike {
  override def explicitCartesian: Boolean = false
  override def sql: String = "INNER"
}

case object Cross extends InnerLike {
  override def explicitCartesian: Boolean = true
  override def sql: String = "CROSS"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI"
}

case object LeftSingle extends JoinType {
  override def sql: String = "LEFT SINGLE"
}

case class ExistenceJoin(exists: Attribute) extends JoinType {
  override def sql: String = {
    // This join type is only used in the end of optimizer and physical plans, we will not
    // generate SQL for this join type
    throw SparkUnsupportedOperationException()
  }
}

case class NaturalJoin(tpe: JoinType) extends JoinType {
  require(Seq(Inner, LeftOuter, RightOuter, FullOuter).contains(tpe),
    "Unsupported natural join type " + tpe)
  override def sql: String = "NATURAL " + tpe.sql
}

case class UsingJoin(tpe: JoinType, usingColumns: Seq[String]) extends JoinType {
  require(Seq(Inner, LeftOuter, LeftSemi, RightOuter, FullOuter, LeftAnti, Cross).contains(tpe),
    "Unsupported using join type " + tpe)
  override def sql: String = "USING " + tpe.sql
  override def toString: String = s"UsingJoin($tpe, ${usingColumns.mkString("[", ", ", "]")})"
}

object LeftExistence {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case j: ExistenceJoin => Some(joinType)
    case _ => None
  }
}

object LeftSemiOrAnti {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case _ => None
  }
}

object AsOfJoinDirection {

  val supported = Seq("forward", "backward", "nearest")

  def apply(direction: String): AsOfJoinDirection = {
    direction.toLowerCase(Locale.ROOT) match {
      case "forward" => Forward
      case "backward" => Backward
      case "nearest" => Nearest
      case _ =>
        throw new AnalysisException(
          errorClass = "AS_OF_JOIN.UNSUPPORTED_DIRECTION",
          messageParameters = Map(
            "direction" -> direction,
            "supported" -> supported.mkString("'", "', '", "'")))
    }
  }
}

sealed abstract class AsOfJoinDirection

case object Forward extends AsOfJoinDirection
case object Backward extends AsOfJoinDirection
case object Nearest extends AsOfJoinDirection

object LateralJoinType {

  val supported = Seq(
    "inner",
    "leftouter", "left", "left_outer",
    "cross"
  )

  def apply(typ: String): JoinType = typ.toLowerCase(Locale.ROOT).replace("_", "") match {
    case "inner" => Inner
    case "leftouter" | "left" => LeftOuter
    case "cross" => Cross
    case _ =>
      throw new AnalysisException(
        errorClass = "UNSUPPORTED_JOIN_TYPE",
        messageParameters = Map(
          "typ" -> typ,
          "supported" -> supported.mkString("'", "', '", "'"))
      )
  }
}

object NearestByDirection {

  val supported = Seq("distance", "similarity")

  def apply(direction: String): NearestByDirection = {
    direction.toLowerCase(Locale.ROOT) match {
      case "distance" => NearestByDistance
      case "similarity" => NearestBySimilarity
      case _ =>
        throw new AnalysisException(
          errorClass = "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION",
          messageParameters = Map(
            "direction" -> direction,
            "supported" -> supported.mkString("'", "', '", "'")))
    }
  }
}

sealed abstract class NearestByDirection

case object NearestByDistance extends NearestByDirection
case object NearestBySimilarity extends NearestByDirection

object NearestByJoinType {

  /** Strings accepted by the Dataset API. */
  val supported = Seq("inner", "leftouter", "left", "left_outer")

  /** Display string used in `NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE` error messages. Matches the
   *  parser-side wording so the same error class reports the same `supported` value across the
   *  SQL and DataFrame paths. */
  val supportedDisplay = "'INNER', 'LEFT OUTER'"

  def apply(typ: String): JoinType = typ.toLowerCase(Locale.ROOT).replace("_", "") match {
    case "inner" => Inner
    case "leftouter" | "left" => LeftOuter
    case _ =>
      throw new AnalysisException(
        errorClass = "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE",
        messageParameters = Map(
          "joinType" -> typ,
          "supported" -> supportedDisplay))
  }
}

object NearestByJoinMode {

  val supported = Seq("approx", "exact")

  /** Returns true for APPROX, false for EXACT. */
  def apply(mode: String): Boolean = mode.toLowerCase(Locale.ROOT) match {
    case "approx" => true
    case "exact" => false
    case _ =>
      throw new AnalysisException(
        errorClass = "NEAREST_BY_JOIN.UNSUPPORTED_MODE",
        messageParameters = Map(
          "mode" -> mode,
          "supported" -> supported.mkString("'", "', '", "'")))
  }
}
