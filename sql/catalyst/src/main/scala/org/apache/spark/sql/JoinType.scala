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

package org.apache.spark.sql

import java.util.Locale

import org.apache.spark.sql.catalyst.plans

/**
 * Enumeration for representing types of joins that can be performed
 */
object JoinType extends Enumeration {
  case class JoinTypeVal private[JoinType] (catalyst: plans.CatalystJoinType) extends Val {
    override def id: Int = super.id
  }

  type Type = JoinTypeVal

  /**
   * Inner Join
   */
  val Inner = JoinTypeVal(plans.Inner)

  /**
   * Cross Join
   */
  val Cross = JoinTypeVal(plans.Cross)

  /**
   * Left Outer (a.k.a. Left) Join
   */
  val LeftOuter = JoinTypeVal(plans.LeftOuter)

  /**
   * Right Outer (a.k.a. Right) Join
   */
  val RightOuter = JoinTypeVal(plans.RightOuter)

  /**
   * Full Outer Join
   */
  val FullOuter = JoinTypeVal(plans.FullOuter)

  /**
   * Left Semi Join
   */
  val LeftSemi = JoinTypeVal(plans.LeftSemi)

  /**
   * Left Anti Join
   */
  val LeftAnti = JoinTypeVal(plans.LeftAnti)

  /**
   * Constructs an instance of [[JoinType.JoinTypeVal]] from a given [[String]] representation
   * @param joinTypeStr the [[String]] representation of the join type
   * @return an instance of [[JoinType.JoinTypeVal]] corresponding to the input
   * @throws [[IllegalArgumentException]] if an unsupported value is provided
   */
  def apply(joinTypeStr: String): JoinType.JoinTypeVal = {
    val sanitizedType = joinTypeStr.toLowerCase(Locale.ROOT).replace("_", "")
    sanitizedType match {
      case "inner" => JoinType.Inner
      case "outer" | "full" | "fullouter" => JoinType.FullOuter
      case "leftouter" | "left" => JoinType.LeftOuter
      case "rightouter" | "right" => JoinType.RightOuter
      case "leftsemi" | "semi" => JoinType.LeftSemi
      case "leftanti" | "anti" => JoinType.LeftAnti
      case "cross" => JoinType.Cross
      case _ =>
        val supported = Seq(
          "inner",
          "outer",
          "full",
          "fullouter",
          "full_outer",
          "leftouter",
          "left",
          "left_outer",
          "rightouter",
          "right",
          "right_outer",
          "leftsemi",
          "left_semi",
          "semi",
          "leftanti",
          "left_anti",
          "anti",
          "cross")

        throw new IllegalArgumentException(
          s"Unsupported join type '$joinTypeStr'. " +
            "Supported join types include: " + supported.mkString("'", "', '", "'") + ".")
    }
  }
}
