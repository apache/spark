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

package org.apache.spark.ml.attribute

/**
 * Keys used to store attributes.
 */
private[attribute] object AttributeKeys {
  val ML_ATTR: String = "ml_attr"
  val TYPE: String = "type"
  val NAME: String = "name"
  val INDEX: String = "idx"
  val MIN: String = "min"
  val MAX: String = "max"
  val STD: String = "std"
  val SPARSITY: String = "sparsity"
  val ORDINAL: String = "ord"
  val VALUES: String = "vals"
  val NUM_VALUES: String = "num_vals"
  val ATTRIBUTES: String = "attrs"
  val NUM_ATTRIBUTES: String = "num_attrs"
}
