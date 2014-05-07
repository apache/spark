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

package org.apache.spark.mllib.regression

import org.apache.spark.mllib.linalg.{Vectors, Vector, VectorParsers}

/**
 * Class that represents the features and labels of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
case class LabeledPoint(label: Double, features: Vector) {
  override def toString: String = {
    Seq(label, features).mkString("(", ",", ")")
  }
}

object LabeledPoint {
  /**
   * Parses a string resulted from `LabeledPoint#toString` into
   * an [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  def parse(s: String) = LabeledPointParsers.parse(s)
}

/**
 * Parsers for string representation of [[org.apache.spark.mllib.regression.LabeledPoint]].
 */
private[mllib] class LabeledPointParsers extends VectorParsers {
  /** Parser for the dense format used before v1.0. */
  lazy val labeledPointV0: Parser[LabeledPoint] =
    floatingPointNumber ~ "," ~ rep(floatingPointNumber) ^^ {
      case l ~ "," ~ vv => LabeledPoint(l.toDouble, Vectors.dense(vv.map(_.toDouble).toArray))
    }
  /** Parser for strings resulted from `LabeledPoint#toString` in v1.0. */
  lazy val labeledPointV1: Parser[LabeledPoint] =
    "(" ~ floatingPointNumber ~ "," ~ vector ~ ")" ^^ {
      case "(" ~ l ~ "," ~ v ~ ")" => LabeledPoint(l.toDouble, v)
    }
  lazy val labeledPoint: Parser[LabeledPoint] = labeledPointV1 | labeledPointV0
}

private[mllib] object LabeledPointParsers extends LabeledPointParsers {
  /** Parses a string into an [[org.apache.spark.mllib.regression.LabeledPoint]]. */
  def parse(s: String): LabeledPoint = parse(labeledPoint, s).get
}
