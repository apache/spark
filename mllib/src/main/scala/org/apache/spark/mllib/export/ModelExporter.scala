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

package org.apache.spark.mllib.export

import java.io.File
import javax.xml.transform.stream.StreamResult

import org.jpmml.model.JAXBUtil

import org.apache.spark.mllib.export.pmml.PMMLModelExport

object ModelExporter {

  /**
  * Export the input model to the stream result in PMML format 
  */
  def toPMML(inputModel: Any, streamResult: StreamResult): Unit = {
    val modelExport = ModelExportFactory.createModelExport(inputModel, ModelExportType.PMML)
    val pmml = modelExport.asInstanceOf[PMMLModelExport].getPmml()
    JAXBUtil.marshalPMML(pmml, streamResult)
  }
  
  /**
  * Export the input model to a local path in PMML format 
  */
  def toPMML(inputModel: Any, localPath: String): Unit = {
    toPMML(inputModel, new StreamResult(new File(localPath)))
  }
  
}
