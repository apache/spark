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

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class SetCommandPhysical(key: Option[String], value: Option[String], output: Seq[Attribute])
                             (@transient context: SQLContext) extends LeafNode {
  def execute(): RDD[Row] = (key, value) match {
    // Set value for key k; the action itself would
    // have been performed in QueryExecution eagerly.
    case (Some(k), Some(v)) => context.emptyResult
    // Query the value bound to key k.
    case (Some(k), None) =>
      val resultString = context.getOption(k) match {
        case Some(v) => s"$k=$v"
        case None => s"$k is undefined"
      }
      context.sparkContext.parallelize(Seq(new GenericRow(Array[Any](resultString))), 1)
    // Query all key-value pairs that are set in the SQLConf of the context.
    case (None, None) =>
      val pairs = context.getAll
      val rows = pairs.map { case (k, v) =>
        new GenericRow(Array[Any](s"$k=$v"))
      }.toSeq
      // Assume config parameters can fit into one split (machine) ;)
      context.sparkContext.parallelize(rows, 1)
    // The only other case is invalid semantics and is impossible.
    case _ => context.emptyResult
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExplainCommandPhysical(child: SparkPlan, output: Seq[Attribute])
                                 (@transient context: SQLContext) extends UnaryNode {
  def execute(): RDD[Row] = {
    val planString = new GenericRow(Array[Any](child.toString))
    context.sparkContext.parallelize(Seq(planString))
  }

  override def otherCopyArgs = context :: Nil
}
