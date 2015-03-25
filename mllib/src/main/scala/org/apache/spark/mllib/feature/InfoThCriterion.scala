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

package org.apache.spark.mllib.feature

/**
 * Trait which declares needed methods to define a criterion for Feature Selection
 */
trait InfoThCriterion extends Serializable with Ordered[InfoThCriterion] {

  var relevance: Double = 0.0

  /**
   * Protected method to set the relevance.
   * The default value is 0.0.
   */
  protected def setRelevance(relevance: Double): InfoThCriterion = {
    this.relevance = relevance
    this
  }

  /** 
   *  Compares the score of two criterions
   */
  override def compare(that: InfoThCriterion) = {
    this.score.compare(that.score)
  }

  /** 
   * Initialize a criterion with a given relevance value
   */
  def init(relevance: Double): InfoThCriterion

  /**
   * 
   * Updates the criterion score with new mutual information and conditional mutual information.
   * @param mi Mutual information between the criterion and another variable.
   * @param cmi Conditional mutual information between the criterion and another variable.
   * 
   */
  def update(mi: Double, cmi: Double): InfoThCriterion

  /**
   * Returns the value of the criterion for in a precise moment.
   */
  def score: Double

}

/**
 * A special type of criterion which can be bounden for optimization.
 */
trait Bound extends Serializable { self: InfoThCriterion =>

  /**
   * Returns the maximum value the criterion can reach given the relevance.
   */
  def bound: Double
}

/**
 * Joint Mutual Information criterion (JMI)
 */
class Jmi extends InfoThCriterion with Bound {

  var redundance: Double = 0.0
  var conditionalRedundance: Double = 0.0
  var selectedSize: Int = 0

  override def bound = 2 * relevance

  override def score = {
    if (selectedSize != 0) {
      relevance - redundance / selectedSize + conditionalRedundance / selectedSize
    } else {
      relevance
    }
  }
  override def init(relevance: Double): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def update(mi: Double, cmi: Double): InfoThCriterion = {
    redundance += mi
    conditionalRedundance += cmi
    selectedSize += 1
    this
  }
  override def toString: String = "JMI"
}

/**
 * Minimum-Redundancy Maximum-Relevance criterion (mRMR)
 */
class Mrmr extends InfoThCriterion with Bound {

  var redundance: Double = 0.0
  var selectedSize: Int = 0

  override def bound = relevance
  override def score = {
    if (selectedSize != 0) {
      relevance - redundance / selectedSize
    } else {
      relevance
    }
  }
  override def init(relevance: Double): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def update(mi: Double, cmi: Double): InfoThCriterion = {
    redundance += mi
    selectedSize += 1
    this
  }
  override def toString: String = "MRMR"
}

/**
 * Conditional Mutual Information Maximization (CMIM)
 */
class Cmim extends InfoThCriterion {

  var modifier: Double = 0.0

  override def score: Double = {
    relevance - modifier
  }
  override def update(mi: Double, cmi: Double): InfoThCriterion = {
    modifier = math.max(modifier, mi - cmi)
    this
  }
  override def init(relevance: Double): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def toString: String = "CMIM"

}


/**
 * Informative Fragments (IF)
 */
class If extends Cmim {
  override def toString: String = "IF"
}


/**
 * Interaction Capping (ICAP)
 */
class Icap extends InfoThCriterion {

  var modifier: Double = 0.0

  override def score: Double = {
    relevance - modifier
  }
  override def update(mi: Double, cmi: Double): InfoThCriterion = {
    modifier += math.max(0.0, mi - cmi)
    this
  }
  override def init(relevance: Double): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def toString: String = "ICAP"
}
