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

package org.apache.spark.scheduler.cluster.mesos

import scala.collection.JavaConverters._

import org.apache.mesos.Protos

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

object MesosProtoUtils extends Logging {

  /** Parses a label string of the format specified in spark.mesos.task.labels. */
  def mesosLabels(labelsStr: String): Protos.Labels.Builder = {

    // Return str split around unescaped occurrences of c.
    def splitUnescaped(str: String, c: Char): Seq[String] = {
      val indices = (0 to str.length - 1).filter {
        i => str(i) == c && (i == 0 || str(i-1) != '\\')
      }

      (-1 +: indices :+ str.length).sliding(2).map {
        case t => str.substring(t(0) + 1, t(1))
      }.toSeq
    }

    val labels = splitUnescaped(labelsStr, ',').map { labelStr =>
      val parts = splitUnescaped(labelStr, ':')
      if (parts.length != 2) {
        throw new SparkException(s"Malformed label: ${labelStr}")
      }

      val cleanedParts = parts
        .map(part => part.replaceAll("\\\\,", ","))
        .map(part => part.replaceAll("\\\\:", ":"))

      Protos.Label.newBuilder().setKey(cleanedParts(0)).setValue(cleanedParts(1)).build()
    }.asJava

    Protos.Labels.newBuilder().addAllLabels(labels)
  }

}
