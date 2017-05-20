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

package org.apache.spark.mllib.clustering.topicmodeling

import org.apache.spark.mllib.feature.Document
import org.apache.spark.rdd.RDD

/**
 * topic modeling interface
 */
trait TopicModel[DocumentParameterType <: DocumentParameters,
                  GlobalParameterType <: GlobalParameters] {
  /**
   *
   * @param documents document collection
   * @return a pair of rdd of document parameters global parameters
   */
  def infer(documents: RDD[Document]): (RDD[DocumentParameterType], GlobalParameterType)

  /**
   *
   * This method performs folding in.
   *
   * It finds distribution of the given documents over topics provided.
   *
   * @param documents  docs to be folded in
   * @param globalParams global parameters that were produced by infer method (stores topics)
   * @return
   */
  def foldIn(documents : RDD[Document],
             globalParams : GlobalParameterType) : RDD[DocumentParameterType]
}
