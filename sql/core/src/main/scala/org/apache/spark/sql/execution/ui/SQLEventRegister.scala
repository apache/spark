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

package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerEventRegister, SparkListener}
import org.apache.spark.ui.SparkUI

class SQLEventRegister extends SparkListenerEventRegister {

  override def getEventClasses(): List[Class[_]] = {
    List(
      classOf[SparkListenerSQLExecutionStart],
      classOf[SparkListenerSQLExecutionEnd])
  }

  override def getListener(): SparkListener = {
    new SQLHistoryListener(new SparkConf())
  }

  override def attachUITab(listener: SparkListener, sparkUI: SparkUI): Unit = {
    new SQLTab(listener.asInstanceOf[SQLListener], sparkUI)
  }
}