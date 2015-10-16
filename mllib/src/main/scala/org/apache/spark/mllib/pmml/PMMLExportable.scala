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

package org.apache.spark.mllib.pmml

import java.io.{File, OutputStream, StringWriter}
import javax.xml.transform.stream.StreamResult

import org.jpmml.model.JAXBUtil

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.mllib.pmml.export.PMMLModelExportFactory

/**
 * :: DeveloperApi ::
 * Export model to the PMML format
 * Predictive Model Markup Language (PMML) is an XML-based file format
 * developed by the Data Mining Group (www.dmg.org).
 */
@DeveloperApi
@Since("1.4.0")
trait PMMLExportable {

  /**
   * Export the model to the stream result in PMML format
   */
  private def toPMML(streamResult: StreamResult): Unit = {
    val pmmlModelExport = PMMLModelExportFactory.createPMMLModelExport(this)
    JAXBUtil.marshalPMML(pmmlModelExport.getPmml, streamResult)
  }

  /**
   * :: Experimental ::
   * Export the model to a local file in PMML format
   */
  @Experimental
  @Since("1.4.0")
  def toPMML(localPath: String): Unit = {
    toPMML(new StreamResult(new File(localPath)))
  }

  /**
   * :: Experimental ::
   * Export the model to a directory on a distributed file system in PMML format
   */
  @Experimental
  @Since("1.4.0")
  def toPMML(sc: SparkContext, path: String): Unit = {
    val pmml = toPMML()
    sc.parallelize(Array(pmml), 1).saveAsTextFile(path)
  }

  /**
   * :: Experimental ::
   * Export the model to the OutputStream in PMML format
   */
  @Experimental
  @Since("1.4.0")
  def toPMML(outputStream: OutputStream): Unit = {
    toPMML(new StreamResult(outputStream))
  }

  /**
   * :: Experimental ::
   * Export the model to a String in PMML format
   */
  @Experimental
  @Since("1.4.0")
  def toPMML(): String = {
    val writer = new StringWriter
    toPMML(new StreamResult(writer))
    writer.toString
  }

}
