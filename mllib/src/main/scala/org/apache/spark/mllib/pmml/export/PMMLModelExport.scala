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

package org.apache.spark.mllib.pmml.`export`

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.beans.BeanProperty

import org.dmg.pmml.{Application, Header, PMML, Timestamp, Version}

private[mllib] trait PMMLModelExport {

  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
      .withZone(ZoneId.systemDefault())

  /**
   * Holder of the exported model in PMML format
   */
  @BeanProperty
  val pmml: PMML = {
    val version = getClass.getPackage.getImplementationVersion
    val app = new Application("Apache Spark MLlib").setVersion(version)
    val timestamp = new Timestamp()
      .addContent(DATE_TIME_FORMATTER.format(Instant.now()))
    val header = new Header()
      .setApplication(app)
      .setTimestamp(timestamp)
    new PMML(Version.PMML_4_4.getVersion(), header, null)
  }
}
