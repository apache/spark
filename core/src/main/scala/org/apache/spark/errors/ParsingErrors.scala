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

package org.apache.spark.errors

import org.json4s.JValue

import org.apache.spark.SparkException

private [spark] object ParsingErrors {
  def ErrorParseJson(json: String, exampleJson: String, e: Throwable): Throwable = {
    new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n" +
      s"Here is a correct example: $exampleJson.", e)
  }

  def ErrorParseJson(json: JValue, e: Throwable): Throwable = {
    new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n", e)
  }

  def errorParsingResource(resourcesFile: String, e: Throwable): Throwable = {
    new SparkException(s"Error parsing resources file $resourcesFile", e)
  }
}
