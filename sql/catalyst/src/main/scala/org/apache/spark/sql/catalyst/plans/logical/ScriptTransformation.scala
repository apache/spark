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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 * @param ioschema the input and output schema applied in the execution of the script.
 */
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: LogicalPlan,
    ioschema: ScriptInputOutputSchema) extends UnaryNode {
  override def references: AttributeSet = AttributeSet(input.flatMap(_.references))

  override def sql: String = {
    val inputRowFormatSQL = ioschema.inputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${ioschema.inputRowFormat}"))
    val outputRowFormatSQL = ioschema.outputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${ioschema.outputRowFormat}"))

    val outputSchema = output.map { attr =>
      s"${attr.sql} ${attr.dataType.simpleString}"
    }.mkString(", ")

    s"SELECT TRANSFORM (${input.map(_.sql).mkString(", ")}) $inputRowFormatSQL " +
      s"USING \'${script}\' AS ($outputSchema) $outputRowFormatSQL " +
      (if (child == OneRowRelation) "" else s"FROM ${child.sql}")
  }
}

/**
 * Input and output properties when passing data to a script.
 * For example, in Hive this would specify which SerDes to use.
 */
case class ScriptInputOutputSchema(
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean) {

  def inputRowFormatSQL: Option[String] =
    getRowFormatSQL(inputRowFormat, inputSerdeClass, inputSerdeProps)

  def outputRowFormatSQL: Option[String] =
    getRowFormatSQL(outputRowFormat, outputSerdeClass, outputSerdeProps)

  /**
   * Get the row format specification
   * Note:
   * 1. Changes are needed when readerClause and writerClause are supported.
   * 2. Changes are needed when "ESCAPED BY" is supported.
   */
  private def getRowFormatSQL(
    rowFormat: Seq[(String, String)],
    serdeClass: Option[String],
    serdeProps: Seq[(String, String)]): Option[String] = {
    if (schemaLess) return Some("")

    val rowFormatDelimited =
      rowFormat.map {
        case ("TOK_TABLEROWFORMATFIELD", value) =>
          "FIELDS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATCOLLITEMS", value) =>
          "COLLECTION ITEMS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATMAPKEYS", value) =>
          "MAP KEYS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATLINES", value) =>
          "LINES TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATNULL", value) =>
          "NULL DEFINED AS " + value
        case o => return None
      }

    val serdeClassSQL = serdeClass.map("'" + _ + "'").getOrElse("")
    val serdePropsSQL =
      if (serdeClass.nonEmpty) {
        val props = serdeProps.map{p => s"'${p._1}' = '${p._2}'"}.mkString(", ")
        if (props.nonEmpty) " WITH SERDEPROPERTIES(" + props + ")" else ""
      } else {
        ""
      }
    if (rowFormat.nonEmpty) {
      Some("ROW FORMAT DELIMITED " + rowFormatDelimited.mkString(" "))
    } else {
      Some("ROW FORMAT SERDE " + serdeClassSQL + serdePropsSQL)
    }
  }
}
