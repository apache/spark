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

package org.apache.spark.ml.source.libsvm

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the LibSVM data source.
 */
private[libsvm] class LibSVMOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  import LibSVMOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * Number of features. If unspecified or nonpositive, the number of features will be determined
   * automatically at the cost of one additional pass.
   */
  val numFeatures = parameters.get(NUM_FEATURES).map(_.toInt).filter(_ > 0)

  val isSparse = parameters.getOrElse(VECTOR_TYPE, SPARSE_VECTOR_TYPE) match {
    case SPARSE_VECTOR_TYPE => true
    case DENSE_VECTOR_TYPE => false
    case o => throw new IllegalArgumentException(s"Invalid value `$o` for parameter " +
      s"`$VECTOR_TYPE`. Expected types are `sparse` and `dense`.")
  }
}

private[libsvm] object LibSVMOptions {
  val NUM_FEATURES = "numFeatures"
  val VECTOR_TYPE = "vectorType"
  val DENSE_VECTOR_TYPE = "dense"
  val SPARSE_VECTOR_TYPE = "sparse"
}
