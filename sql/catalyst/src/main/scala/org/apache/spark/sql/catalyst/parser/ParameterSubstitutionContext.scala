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
package org.apache.spark.sql.catalyst.parser

/**
 * Thread-local storage for parameter substitution information.
 *
 * This allows the main parser to be aware of parameter substitution that happened
 * before parsing, enabling it to create correct error contexts from the beginning
 * instead of requiring post-processing translation.
 *
 * The substitution context contains:
 * - Original SQL text (before parameter substitution)
 * - Substituted SQL text (after parameter substitution)
 * - Position mapping information for translating positions between the two
 */
object ParameterSubstitutionContext {

  /**
   * Information about parameter substitution that occurred before parsing.
   *
   * @param originalSql The original SQL text with parameter markers
   * @param substitutedSql The SQL text after parameter substitution
   * @param positionMapper The mapper to translate positions between original and substituted text
   */
  case class SubstitutionInfo(
      originalSql: String,
      substitutedSql: String,
      positionMapper: PositionMapper)

  private val substitutionInfo = new ThreadLocal[Option[SubstitutionInfo]]() {
    override def initialValue(): Option[SubstitutionInfo] = None
  }

  /**
   * Set the parameter substitution information for the current thread.
   * This should be called by parameter handlers before invoking the main parser.
   *
   * @param info The substitution information
   */
  def setSubstitutionInfo(info: SubstitutionInfo): Unit = {
    substitutionInfo.set(Some(info))
  }

  /**
   * Get the parameter substitution information for the current thread.
   * This is called by the parser to adjust error contexts.
   *
   * @return The substitution information, if any
   */
  def getSubstitutionInfo(): Option[SubstitutionInfo] = {
    substitutionInfo.get()
  }

  /**
   * Clear the parameter substitution information for the current thread.
   * This should be called after parsing is complete to prevent memory leaks.
   */
  def clearSubstitutionInfo(): Unit = {
    substitutionInfo.remove()
  }

  /**
   * Execute a block of code with parameter substitution information set.
   * The information is automatically cleared after the block completes.
   *
   * @param info The substitution information to set
   * @param f The block of code to execute
   * @return The result of the block
   */
  def withSubstitutionInfo[T](info: SubstitutionInfo)(f: => T): T = {
    setSubstitutionInfo(info)
    try {
      f
    } finally {
      clearSubstitutionInfo()
    }
  }
}

