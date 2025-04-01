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

package org.apache.spark.sql.connect

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto.{NAReplace, Relation}
import org.apache.spark.connect.proto.Expression.{Literal => GLiteral}
import org.apache.spark.connect.proto.NAReplace.Replacement
import org.apache.spark.sql
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.toLiteral
import org.apache.spark.sql.connect.ConnectConversions._

/**
 * Functionality for working with missing data in `DataFrame`s.
 *
 * @since 3.4.0
 */
final class DataFrameNaFunctions private[sql] (sparkSession: SparkSession, root: Relation)
    extends sql.DataFrameNaFunctions {

  override protected def drop(minNonNulls: Option[Int]): DataFrame =
    buildDropDataFrame(None, minNonNulls)

  override protected def drop(minNonNulls: Option[Int], cols: Seq[String]): DataFrame =
    buildDropDataFrame(Option(cols), minNonNulls)

  private def buildDropDataFrame(
      cols: Option[Seq[String]],
      minNonNulls: Option[Int]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val dropNaBuilder = builder.getDropNaBuilder.setInput(root)
      cols.foreach(c => dropNaBuilder.addAllCols(c.asJava))
      minNonNulls.foreach(dropNaBuilder.setMinNonNulls)
    }
  }

  /** @inheritdoc */
  def fill(value: Long): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setLong(value).build())
  }

  /** @inheritdoc */
  def fill(value: Long, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setLong(value).build())
  }

  /** @inheritdoc */
  def fill(value: Double): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setDouble(value).build())
  }

  /** @inheritdoc */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setDouble(value).build())
  }

  /** @inheritdoc */
  def fill(value: String): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setString(value).build())
  }

  /** @inheritdoc */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setString(value).build())
  }

  /** @inheritdoc */
  def fill(value: Boolean): DataFrame = {
    buildFillDataFrame(None, GLiteral.newBuilder().setBoolean(value).build())
  }

  /** @inheritdoc */
  def fill(value: Boolean, cols: Seq[String]): DataFrame = {
    buildFillDataFrame(Some(cols), GLiteral.newBuilder().setBoolean(value).build())
  }

  private def buildFillDataFrame(cols: Option[Seq[String]], value: GLiteral): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val fillNaBuilder = builder.getFillNaBuilder.setInput(root)
      fillNaBuilder.addValues(value)
      cols.foreach(c => fillNaBuilder.addAllCols(c.asJava))
    }
  }

  protected def fillMap(values: Seq[(String, Any)]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val fillNaBuilder = builder.getFillNaBuilder.setInput(root)
      values.map { case (colName, replaceValue) =>
        fillNaBuilder.addCols(colName).addValues(toLiteral(replaceValue).getLiteral)
      }
    }
  }

  /** @inheritdoc */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    val cols = if (col != "*") Some(Seq(col)) else None
    buildReplaceDataFrame(cols, buildReplacement(replacement))
  }

  /** @inheritdoc */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    buildReplaceDataFrame(Some(cols), buildReplacement(replacement))
  }

  private def buildReplaceDataFrame(
      cols: Option[Seq[String]],
      replacements: Iterable[NAReplace.Replacement]): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val replaceBuilder = builder.getReplaceBuilder.setInput(root)
      replaceBuilder.addAllReplacements(replacements.asJava)
      cols.foreach(c => replaceBuilder.addAllCols(c.asJava))
    }
  }

  private def buildReplacement[T](replacement: Map[T, T]): Iterable[NAReplace.Replacement] = {
    // Convert the NumericType in replacement map to DoubleType,
    // while leaving StringType, BooleanType and null untouched.
    val replacementMap: Map[_, _] = replacement.map {
      case (k, v: String) => (k, v)
      case (k, v: Boolean) => (k, v)
      case (k: String, null) => (k, null)
      case (k: Boolean, null) => (k, null)
      case (k, null) => (convertToDouble(k), null)
      case (k, v) => (convertToDouble(k), convertToDouble(v))
    }
    replacementMap.map { case (oldValue, newValue) =>
      Replacement
        .newBuilder()
        .setOldValue(toLiteral(oldValue).getLiteral)
        .setNewValue(toLiteral(newValue).getLiteral)
        .build()
    }
  }

  private def convertToDouble(v: Any): Double = v match {
    case v: Float => v.toDouble
    case v: Double => v
    case v: Long => v.toDouble
    case v: Int => v.toDouble
    case v =>
      throw new IllegalArgumentException(s"Unsupported value type ${v.getClass.getName} ($v).")
  }

  /** @inheritdoc */
  override def drop(): DataFrame = super.drop()

  /** @inheritdoc */
  override def drop(cols: Array[String]): DataFrame = super.drop(cols)

  /** @inheritdoc */
  override def drop(cols: Seq[String]): DataFrame = super.drop(cols)

  /** @inheritdoc */
  override def drop(how: String, cols: Array[String]): DataFrame = super.drop(how, cols)

  /** @inheritdoc */
  override def drop(minNonNulls: Int, cols: Array[String]): DataFrame =
    super.drop(minNonNulls, cols)

  /** @inheritdoc */
  override def drop(how: String): DataFrame = super.drop(how)

  /** @inheritdoc */
  override def drop(how: String, cols: Seq[String]): DataFrame = super.drop(how, cols)

  /** @inheritdoc */
  override def drop(minNonNulls: Int): DataFrame = super.drop(minNonNulls)

  /** @inheritdoc */
  override def drop(minNonNulls: Int, cols: Seq[String]): DataFrame =
    super.drop(minNonNulls, cols)

  /** @inheritdoc */
  override def fill(value: Long, cols: Array[String]): DataFrame = super.fill(value, cols)

  /** @inheritdoc */
  override def fill(value: Double, cols: Array[String]): DataFrame = super.fill(value, cols)

  /** @inheritdoc */
  override def fill(value: String, cols: Array[String]): DataFrame = super.fill(value, cols)

  /** @inheritdoc */
  override def fill(value: Boolean, cols: Array[String]): DataFrame = super.fill(value, cols)

  /** @inheritdoc */
  override def fill(valueMap: java.util.Map[String, Any]): DataFrame = super.fill(valueMap)

  /** @inheritdoc */
  override def fill(valueMap: Map[String, Any]): DataFrame = super.fill(valueMap)

  /** @inheritdoc */
  override def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame =
    super.replace[T](col, replacement)

  /** @inheritdoc */
  override def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame =
    super.replace(cols, replacement)
}
