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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Attribute}

trait Command {
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[Any] = Seq.empty[Any]
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class SetCommand(
    key: Option[String], value: Option[String], output: Seq[Attribute])(
    @transient context: SQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult: Seq[(String, String)] = (key, value) match {
    // Set value for key k.
    case (Some(k), Some(v)) =>
      context.set(k, v)
      Array(k -> v)

    // Query the value bound to key k.
    case (Some(k), _) =>
      Array(k -> context.getOption(k).getOrElse("<undefined>"))

    // Query all key-value pairs that are set in the SQLConf of the context.
    case (None, None) =>
      context.getAll

    case _ =>
      throw new IllegalArgumentException()
  }

  def execute(): RDD[Row] = {
    val rows = sideEffectResult.map { case (k, v) => new GenericRow(Array[Any](k, v)) }
    context.sparkContext.parallelize(rows, 1)
  }

  override def otherCopyArgs = context :: Nil
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExplainCommand(
    child: SparkPlan, output: Seq[Attribute])(
    @transient context: SQLContext)
  extends UnaryNode with Command {

  // Actually "EXPLAIN" command doesn't cause any side effect.
  override protected[sql] lazy val sideEffectResult: Seq[String] = this.toString.split("\n")

  def execute(): RDD[Row] = {
    val explanation = sideEffectResult.map(row => new GenericRow(Array[Any](row)))
    context.sparkContext.parallelize(explanation, 1)
  }

  override def otherCopyArgs = context :: Nil
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CacheCommand(tableName: String, doCache: Boolean)(@transient context: SQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    if (doCache) {
      context.cacheTable(tableName)
    } else {
      context.uncacheTable(tableName)
    }
    Seq.empty[Any]
  }

  override def execute(): RDD[Row] = {
    sideEffectResult
    context.emptyResult
  }

  override def output: Seq[Attribute] = Seq.empty
}
