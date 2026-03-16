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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Common utilities for generator resolution.
 */
object GeneratorResolution {
  /**
   * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
   * names is empty names are assigned from field names in generator.
   *
   * @throws AnalysisException UDTF_ALIAS_NUMBER_MISMATCH if the number of names does not match
   *                           the number of output attributes from the generator
   */
  def makeGeneratorOutput(
      generator: Generator,
      names: Seq[String]): Seq[Attribute] = {
    val elementAttrs = DataTypeUtils.toAttributes(generator.elementSchema)

    if (names.length == elementAttrs.length) {
      names.zip(elementAttrs).map {
        case (name, attr) => attr.withName(name)
      }
    } else if (names.isEmpty) {
      elementAttrs
    } else {
      throw QueryCompilationErrors.aliasesNumberNotMatchUDTFOutputError(
        elementAttrs.size, names.mkString(","))
    }
  }
}
