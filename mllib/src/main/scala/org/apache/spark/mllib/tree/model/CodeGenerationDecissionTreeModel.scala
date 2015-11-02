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

package org.apache.spark.mllib.tree.model

import scala.collection.mutable

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.util.Utils

/**
 * Special code generated version of the [[DecisionTreeModel]].
 * This model should be used when the number of trees are relatively
 * small (DB's testing from SPARK-10387 indicates performance starts
 * to degrade above 500 trees).
 * Decision tree model for classification or regression.
 * This model stores the decision tree structure and parameters.
 * @param topNode root node
 * @param algo algorithm type -- classification or regression
 */
@Since("1.6.0")
class CodeGenerationDecisionTreeModel @Since("1.6.0") (
    @Since("1.6.0") val topNode: Node,
    @Since("1.6.0") val algo: Algo) extends DecisionTreeModel(topNode, algo) {
}

object CodeGenerationDecisionTreeModel {
  def apply(model: DecisionTreeModel): CodeGenerationDecisionTreeModel = {
    CodeGenerationDecisionTreeModel(model.topNode, model.algo)
  }
}
