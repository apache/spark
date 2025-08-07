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

package org.apache.spark.kafka010

import org.apache.spark.{ErrorClassesJsonReader, SparkException}

private object ExceptionsHelper {
  val errorJsonReader: ErrorClassesJsonReader =
    new ErrorClassesJsonReader(
      // Note that though we call them "error classes" here, the proper name is "error conditions",
      // hence why the name of the JSON file is different. We will address this inconsistency as
      // part of this ticket: https://issues.apache.org/jira/browse/SPARK-47429
      Seq(getClass.getClassLoader.getResource("error/kafka-token-provider-error-conditions.json")))
}

object KafkaTokenProviderExceptions {
  def missingKafkaOption(option: String): SparkException = {
    val errClass = "MISSING_KAFKA_OPTION"
    val param = Map("option" -> option)

    new SparkException(
      message = ExceptionsHelper.errorJsonReader.getErrorMessage(
        errClass,
        param),
      cause = null,
      errorClass = Some(errClass),
      messageParameters = param
    )
  }
}
