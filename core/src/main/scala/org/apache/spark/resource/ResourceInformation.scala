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

package org.apache.spark.resource

import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction, Formats, JValue}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving
import org.apache.spark.util.ArrayImplicits._

/**
 * Class to hold information about a type of Resource. A resource could be a GPU, FPGA, etc.
 * The array of addresses are resource specific and its up to the user to interpret the address.
 *
 * One example is GPUs, where the addresses would be the indices of the GPUs
 *
 * @param name the name of the resource
 * @param addresses an array of strings describing the addresses of the resource
 *
 * @since 3.0.0
 */
@Evolving
class ResourceInformation(
    val name: String,
    val addresses: Array[String]) extends Serializable {

  override def toString: String = s"[name: ${name}, addresses: ${addresses.mkString(",")}]"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceInformation =>
        that.getClass == this.getClass &&
        that.name == name &&
        that.addresses.toImmutableArraySeq == addresses.toImmutableArraySeq
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(name, addresses.toImmutableArraySeq).hashCode()

  // TODO(SPARK-39658): reconsider whether we want to expose a third-party library's
  // symbols as part of a public API:
  final def toJson(): JValue = ResourceInformationJson(name, addresses.toImmutableArraySeq).toJValue
}

private[spark] object ResourceInformation {

  private lazy val exampleJson: String = compact(render(
    ResourceInformationJson("gpu", Seq("0", "1")).toJValue))

  /**
   * Parses a JSON string into a [[ResourceInformation]] instance.
   */
  def parseJson(json: String): ResourceInformation = {
    implicit val formats: Formats = DefaultFormats
    try {
      parse(json).extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n" +
          s"Here is a correct example: $exampleJson.", e)
    }
  }

  def parseJson(json: JValue): ResourceInformation = {
    implicit val formats: Formats = DefaultFormats
    try {
      json.extract[ResourceInformationJson].toResourceInformation
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n", e)
    }
  }
}

/** A case class to simplify JSON serialization of [[ResourceInformation]]. */
private case class ResourceInformationJson(name: String, addresses: Seq[String]) {

  def toJValue: JValue = {
    Extraction.decompose(this)(DefaultFormats)
  }

  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(name, addresses.toArray)
  }
}
