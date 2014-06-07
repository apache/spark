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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Attribute}

case class ExplainCommandPhysical(child: SparkPlan)
                                 (@transient context: SQLContext) extends UnaryNode {
  def execute(): RDD[Row] = {
    val planString = new GenericRow(Array[Any](child.toString))
    context.sparkContext.parallelize(Seq(planString))
  }

  def output: Seq[Attribute] = child.output // right thing to do?

  override def otherCopyArgs = context :: Nil
}
