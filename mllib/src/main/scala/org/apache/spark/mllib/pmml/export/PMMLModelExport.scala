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

package org.apache.spark.mllib.pmml.export

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.beans.BeanProperty

import org.dmg.pmml.{Application, Header, PMML, Timestamp}

private[mllib] trait PMMLModelExport {

  /**
   * Holder of the exported model in PMML format
   */
  @BeanProperty
  val pmml: PMML = {
    val version = getClass.getPackage.getImplementationVersion
    val app = new Application("Apache Spark MLlib").setVersion(version)
    val timestamp = new Timestamp()
      .addContent(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).format(new Date()))
    val header = new Header()
      .setApplication(app)
      .setTimestamp(timestamp)
    new PMML("4.2", header, null)
  }
}
