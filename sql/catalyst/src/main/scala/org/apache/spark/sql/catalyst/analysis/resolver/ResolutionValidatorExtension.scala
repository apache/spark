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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Extension interface for the resolution validator. Allows external code to register additional
 * operator validation checks without modifying [[ResolutionValidator]] directly.
 */
trait ResolutionValidatorExtension {

  /**
   * Validate the given resolved operator if this extension handles it.
   *
   * Extensions that validate a leaf operator must publish its output to the validator's
   * attribute scope via [[ResolutionValidator.handleOperatorOutput]], so that attribute
   * references in parent operators pass the scope checks.
   *
   * @return true if this extension handles and validates the operator, false otherwise.
   */
  def validateOperator(operator: LogicalPlan, validator: ResolutionValidator): Boolean
}
