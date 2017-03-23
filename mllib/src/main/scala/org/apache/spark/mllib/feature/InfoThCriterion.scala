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

  var relevance: Float = 0.0f
  var valid: Boolean = true 

  /**
   * Protected method to set the relevance.
   * The default value is 0.0.
   */
  protected def setRelevance(relevance: Float): InfoThCriterion = {
    this.relevance = relevance
    this
  }
  
  /**
   * Method to set the validity.
   * The default value is true.
   */
  def setValid(valid: Boolean): InfoThCriterion = {
    this.valid = valid
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
  def init(relevance: Float): InfoThCriterion

  /**
   * 
   * Updates the criterion score with new mutual information and conditional mutual information.
   * @param mi Mutual information between the criterion and another variable.
   * @param cmi Conditional mutual information between the criterion and another variable.
   * 
   */
  def update(mi: Float, cmi: Float): InfoThCriterion

  /**
   * Returns the value of the criterion for in a precise moment.
   */
  def score: Float
}


/**
 * Mutual Information Maximisation (MIM)
 */
class Mim extends InfoThCriterion {

  override def score = relevance
  
  override def init(relevance: Float): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  
  override def update(mi: Float = 0.0f, cmi: Float = 0.0f): InfoThCriterion = this
  override def toString: String = "MIM"
}

/**
 * Mutual Information FS (MIFS)
 */
class Mifs (val beta: Float = 0.0f) extends InfoThCriterion {

  var redundance: Float = 0.0f

  override def score = relevance - redundance * beta
  
  override def init(relevance: Float): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  
  override def update(mi: Float, cmi: Float = 0.0f): InfoThCriterion = {
    redundance += mi
    this
  }
  
  override def toString: String = "MIFS"
}


/**
 * Joint Mutual Information criterion (JMI)
 */
class Jmi extends InfoThCriterion {

  var redundance: Float = 0.0f
  var conditionalRedundance: Float = 0.0f
  var selectedSize: Int = 0

  override def score = {
    if (selectedSize != 0) {
      relevance - redundance / selectedSize + conditionalRedundance / selectedSize
    } else {
      relevance
    }
  }
  override def init(relevance: Float): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def update(mi: Float, cmi: Float): InfoThCriterion = {
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
class Mrmr extends InfoThCriterion {

  var redundance: Float = 0.0f
  var selectedSize: Int = 0

  override def score = {
    if (selectedSize != 0) {
      relevance - redundance / selectedSize
    } else {
      relevance
    }
  }
  override def init(relevance: Float): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def update(mi: Float, cmi: Float = 0.0f): InfoThCriterion = {
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

  var modifier: Float = 0.0f

  override def score: Float = {
    relevance - modifier
  }
  override def update(mi: Float, cmi: Float): InfoThCriterion = {
    modifier = math.max(modifier, mi - cmi)
    this
  }
  override def init(relevance: Float): InfoThCriterion = {
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

  var modifier: Float = 0.0f

  override def score: Float = {
    relevance - modifier
  }
  override def update(mi: Float, cmi: Float): InfoThCriterion = {
    modifier += math.max(0.0f, mi - cmi)
    this
  }
  override def init(relevance: Float): InfoThCriterion = {
    this.setRelevance(relevance)
  }
  override def toString: String = "ICAP"
}
