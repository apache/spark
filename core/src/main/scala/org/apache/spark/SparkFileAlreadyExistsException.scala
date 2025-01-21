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

package org.apache.spark

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileAlreadyExistsException

/**
 * Hadoop file already exists exception thrown from Spark with an error class.
 */
private[spark] class SparkFileAlreadyExistsException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends FileAlreadyExistsException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters))
    with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}
