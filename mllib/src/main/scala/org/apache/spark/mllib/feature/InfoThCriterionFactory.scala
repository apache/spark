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
 * Factory class that generates a wide range of info-theory criterions [1] to 
 * perform a feature selection phase on data.
 * 
 * [1] Brown, G., Pocock, A., Zhao, M. J., & Lujn, M. (2012). 
 * "Conditional likelihood maximization: a unifying framework 
 * for information theoretic feature selection." 
 * The Journal of Machine Learning Research, 13(1), 27-66.
 * 
 * @param criterion String that specifies the criterion to be used 
 * (options: JMI, mRMR, ICAP, CMIM and IF).
 * @return An initialized info-theory criterion.
 * 
 */

class InfoThCriterionFactory(val criterion: String) extends Serializable {

  val JMI  = "jmi"
  val MRMR = "mrmr"
  val ICAP = "icap"
  val CMIM = "cmim"
  val IF   = "if"

  /** 
   *  Generates a specific info-theory criterion
   */
  def getCriterion: InfoThCriterion = {
    criterion match {
      case JMI  => new Jmi
      case MRMR => new Mrmr
      case ICAP => new Icap
      case CMIM => new Cmim
      case IF   => new If
      case _    => throw new IllegalArgumentException("criterion unknown")
    }
  }
}
