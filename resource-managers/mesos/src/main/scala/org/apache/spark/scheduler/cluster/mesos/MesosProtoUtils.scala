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

import org.apache.mesos.Protos

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

object MesosProtoUtils extends Logging {

  /** Parses a label string of the format specified in spark.mesos.task.labels. */
  def mesosLabels(labelsStr: String): Protos.Labels.Builder = {
    var key: Option[String] = None
    var value: Option[String] = None
    var currStr = ""
    var i = 0
    val labels = Protos.Labels.newBuilder()

    // 0 -> parsing key
    // 1 -> parsing value
    var state = 0

    def addLabel() = {
      value = Some(currStr)
      if (key.isEmpty) {
        throw new SparkException(s"Error while parsing label string: ${labelsStr}.  " +
          s"Empty label key.")
      } else {
        val label = Protos.Label.newBuilder().setKey(key.get).setValue(value.get)
        labels.addLabels(label)

        key = None
        value = None
        currStr = ""
        state = 0
      }
    }

    while(i < labelsStr.length) {
      val c = labelsStr(i)

      if (c == ',') {
        addLabel()
      } else if (c == ':') {
        key = Some(currStr)
        currStr = ""
        state = 1
      } else if (c == '\\') {
        if (i == labelsStr.length - 1) {
          if (state == 1) {
            value = value.map(_ + '\\')
          } else {
            throw new SparkException(s"Error while parsing label string: ${labelsStr}.  " +
              "Key has no value.")
          }
        } else {
          val c2 = labelsStr(i + 1)
          if (c2 == ',' || c2 == ':') {
            currStr += c2
            i += 1
          } else {
            currStr += c
          }
        }
      } else {
        currStr += c
      }

      i += 1
    }

    addLabel()
    labels
  }
}
