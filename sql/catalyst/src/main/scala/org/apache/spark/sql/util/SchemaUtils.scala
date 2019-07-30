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

package org.apache.spark.sql.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.expressions.{NamedTransform, Ref, Transform}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types._


/**
 * Utils for handling schemas.
 *
 * TODO: Merge this file with [[org.apache.spark.ml.util.SchemaUtils]].
 */
private[spark] object SchemaUtils {

  /**
   * Checks if an input schema has duplicate column names. This throws an exception if the
   * duplication exists.
   *
   * @param schema schema to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkSchemaColumnNameDuplication(
      schema: StructType, colType: String, caseSensitiveAnalysis: Boolean = false): Unit = {
    checkColumnNameDuplication(schema.map(_.name), colType, caseSensitiveAnalysis)
  }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) {
      true
    } else if (resolver == caseInsensitiveResolution) {
      false
    } else {
      sys.error("A resolver to check if two identifiers are equal must be " +
        "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst.")
    }
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    // scalastyle:off caselocale
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    // scalastyle:on caselocale
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    def explode(schema: StructType): Seq[Seq[String]] = {
      def recurseIntoComplexTypes(complexType: DataType): Seq[Seq[String]] = {
        complexType match {
          case s: StructType => explode(s)
          case a: ArrayType => recurseIntoComplexTypes(a.elementType)
          case m: MapType =>
            recurseIntoComplexTypes(m.keyType).map(Seq("key") ++ _) ++
              recurseIntoComplexTypes(m.valueType).map(Seq("value") ++ _)
          case _ => Nil
        }
      }

      schema.flatMap {
        case StructField(name, s: StructType, _, _) =>
          Seq(Seq(name)) ++ explode(s).map(nested => Seq(name) ++ nested)
        case StructField(name, a: ArrayType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(a).map(nested => Seq(name) ++ nested)
        case StructField(name, m: MapType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(m).map(nested => Seq(name) ++ nested)
        case f => Seq(f.name) :: Nil
      }
    }

    explode(schema).map(UnresolvedAttribute.apply(_).name)
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param schema the schema to check for duplicates
   * @param checkType contextual information around the check, used in an exception message
   */
  def checkV2ColumnNameDuplication(
      schema: StructType,
      checkType: String,
      isCaseSensitive: Boolean): Unit = {
    val columnNames = explodeNestedFieldNames(schema)
    checkColumnNameDuplication(columnNames, checkType, isCaseSensitive)
  }

  def checkTransformDuplication(
      transforms: Seq[Transform],
      checkType: String,
      isCaseSensitive: Boolean): Unit = {
    val normalizedTransforms = if (isCaseSensitive) {
      transforms.map {
        case NamedTransform(transformName, refs) =>
          val fieldNameParts = refs.collect { case Ref(parts) => parts}
          transformName -> fieldNameParts
      }
    } else {
      transforms.map {
        case NamedTransform(transformName, refs) =>
          // scalastyle:off caselocale
          val fieldNameParts = refs.collect { case Ref(parts) => parts.map(_.toLowerCase)}
          // scalastyle:on caselocale
          transformName -> fieldNameParts
      }
    }
    println(normalizedTransforms)
    println(normalizedTransforms.distinct)
    if (normalizedTransforms.distinct.length != normalizedTransforms.length) {
      val duplicateColumns = normalizedTransforms.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"${x._2.mkString(".")}"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $checkType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * Returns the given column's ordinal within the given `schema`. The length of the returned
   * position will be as long as how nested the column is.
   *
   * @param column The column to search for in the given struct. If the length of `column` is
   *               greater than 1, we expect to enter a nested field.
   * @param schema The current struct we are looking at.
   * @param resolver The resolver to find the column.
   */
  def findColumnPosition(
      column: Seq[String],
      schema: StructType,
      resolver: Resolver): Seq[Int] = {
    def find(column: Seq[String], schema: StructType, stack: Seq[String]): Seq[Int] = {
      if (column.isEmpty) return Nil
      val thisCol = column.head
      lazy val columnPath = UnresolvedAttribute(stack :+ thisCol).name
      val pos = schema.indexWhere(f => resolver(f.name, thisCol))
      if (pos == -1) {
        throw new IndexOutOfBoundsException(columnPath)
      }
      val children = schema(pos).dataType match {
        case s: StructType =>
          find(column.tail, s, stack :+ thisCol)
        case ArrayType(s: StructType, _) =>
          find(column.tail, s, stack :+ thisCol)
        case o =>
          if (column.length > 1) {
            throw new AnalysisException(
              s"""Expected $columnPath to be a nested data type, but found $o. Was looking for the
                 |index of ${UnresolvedAttribute(column).name} in a nested field
              """.stripMargin)
          }
          Nil
      }
      Seq(pos) ++ children
    }

    try {
      find(column, schema, Nil)
    } catch {
      case i: IndexOutOfBoundsException =>
        throw new AnalysisException(
          s"Couldn't find column ${i.getMessage} in:\n${schema.treeString}")
      case e: AnalysisException =>
        throw new AnalysisException(e.getMessage + s":\n${schema.treeString}")
    }
  }
}
