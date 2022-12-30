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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.{FieldName, FieldPosition}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

/**
 * Type to keep track of Hive serde info
 */
case class SerdeInfo(
    storedAs: Option[String] = None,
    formatClasses: Option[FormatClasses] = None,
    serde: Option[String] = None,
    serdeProperties: Map[String, String] = Map.empty) {
  // this uses assertions because validation is done in validateRowFormatFileFormat etc.
  assert(storedAs.isEmpty || formatClasses.isEmpty,
    "Cannot specify both STORED AS and INPUTFORMAT/OUTPUTFORMAT")

  def describe: String = {
    val serdeString = if (serde.isDefined || serdeProperties.nonEmpty) {
      "ROW FORMAT " + serde.map(sd => s"SERDE $sd").getOrElse("DELIMITED")
    } else {
      ""
    }

    this match {
      case SerdeInfo(Some(storedAs), _, _, _) =>
        s"STORED AS $storedAs $serdeString"
      case SerdeInfo(_, Some(formatClasses), _, _) =>
        s"STORED AS $formatClasses $serdeString"
      case _ =>
        serdeString
    }
  }

  def merge(other: SerdeInfo): SerdeInfo = {
    def getOnly[T](desc: String, left: Option[T], right: Option[T]): Option[T] = {
      (left, right) match {
        case (Some(l), Some(r)) =>
          assert(l == r, s"Conflicting $desc values: $l != $r")
          left
        case (Some(_), _) =>
          left
        case (_, Some(_)) =>
          right
        case _ =>
          None
      }
    }

    SerdeInfo.checkSerdePropMerging(serdeProperties, other.serdeProperties)
    SerdeInfo(
      getOnly("STORED AS", storedAs, other.storedAs),
      getOnly("INPUTFORMAT/OUTPUTFORMAT", formatClasses, other.formatClasses),
      getOnly("SERDE", serde, other.serde),
      serdeProperties ++ other.serdeProperties)
  }
}

case class FormatClasses(input: String, output: String) {
  override def toString: String = s"INPUTFORMAT $input OUTPUTFORMAT $output"
}

object SerdeInfo {
  val empty: SerdeInfo = SerdeInfo(None, None, None, Map.empty)

  def checkSerdePropMerging(
      props1: Map[String, String], props2: Map[String, String]): Unit = {
    val conflictKeys = props1.keySet.intersect(props2.keySet)
    if (conflictKeys.nonEmpty) {
      throw QueryExecutionErrors.cannotSafelyMergeSerdePropertiesError(props1, props2, conflictKeys)
    }
  }
}

/**
 * Column data as parsed by ALTER TABLE ... (ADD|REPLACE) COLUMNS.
 */
case class QualifiedColType(
    path: Option[FieldName],
    colName: String,
    dataType: DataType,
    nullable: Boolean,
    comment: Option[String],
    position: Option[FieldPosition],
    default: Option[String]) {
  def name: Seq[String] = path.map(_.name).getOrElse(Nil) :+ colName

  def resolved: Boolean = path.forall(_.resolved) && position.forall(_.resolved)
}
