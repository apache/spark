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

package org.apache.spark.mllib.export.pmml

import org.apache.spark.mllib.export.ModelExport
import java.io.OutputStream
import org.jpmml.model.JAXBUtil
import org.dmg.pmml.PMML
import javax.xml.transform.stream.StreamResult
import scala.beans.BeanProperty
import org.dmg.pmml.Application
import org.dmg.pmml.Timestamp
import org.dmg.pmml.Header
import java.text.SimpleDateFormat
import java.util.Date

trait PMMLModelExport extends ModelExport{
  
  /**
   * Holder of the exported model in PMML format
   */
  @BeanProperty
  var pmml: PMML = new PMML();

  setHeader(pmml);
  
  private def setHeader(pmml : PMML): Unit = {
    var version = getClass().getPackage().getImplementationVersion()
    var app = new Application().withName("Apache Spark MLlib").withVersion(version)
    var timestamp = new Timestamp()
        .withContent(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()))
    var header = new Header()
        .withCopyright("www.dmg.org")
        .withApplication(app)
        .withTimestamp(timestamp);
    pmml.setHeader(header);
  } 
  
  /**
   * Write the exported model (in PMML XML) to the output stream specified 
   */
  @Override
  def save(outputStream: OutputStream): Unit = {
    JAXBUtil.marshalPMML(pmml, new StreamResult(outputStream));  
  }
  
}
