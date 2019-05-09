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
package org.apache.spark.ml.util

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.dmg.pmml.PMML
import org.jpmml.model.{JAXBUtil, SAXUtil}
import org.jpmml.model.filters.ImportFilter

/**
 * Testing utils for working with PMML.
 * Predictive Model Markup Language (PMML) is an XML-based file format
 * developed by the Data Mining Group (www.dmg.org).
 */
private[spark] object PMMLUtils {
  /**
   * :: Experimental ::
   * Load a PMML model from a string. Note: for testing only, PMML model evaluation is supported
   * through external spark-packages.
   */
  def loadFromString(input: String): PMML = {
    val transformed = SAXUtil.createFilteredSource(
      new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
      new ImportFilter())
    JAXBUtil.unmarshalPMML(transformed)
  }
}
