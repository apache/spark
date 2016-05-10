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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, ShuffledRowRDD, SparkPlan}

/**
 * FragmentInput is the leaf node of parent fragment that connect with an child fragment.
 */
case class FragmentInput(@transient childFragment: QueryFragment) extends LeafExecNode {

  private[this] var optimized: Boolean = false

  private[this] var inputPlan: SparkPlan = null

  private[this] var shuffledRdd: ShuffledRowRDD = null

  override def output: Seq[Attribute] = inputPlan.output

  private[sql] def setOptimized() = {
    this.optimized = true
  }

  private[sql] def isOptimized(): Boolean = this.optimized

  private[sql] def setShuffleRdd(shuffledRdd: ShuffledRowRDD) = {
    this.shuffledRdd = shuffledRdd
  }

  private[sql] def setInputPlan(inputPlan: SparkPlan) = {
    this.inputPlan = inputPlan
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (shuffledRdd != null) {
      shuffledRdd
    } else {
      inputPlan.execute()
    }
  }

  override def simpleString: String = "FragmentInput"

  override def innerChildren: Seq[SparkPlan] = inputPlan :: Nil
}
