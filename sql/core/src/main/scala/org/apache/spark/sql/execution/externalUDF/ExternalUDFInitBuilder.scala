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

package org.apache.spark.sql.execution.externalUDF

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import org.apache.spark.sql.catalyst.expressions.{Expression, ExternalUserDefinedFunction,
  NamedArgumentExpression}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.udf.worker.{Init, PropertyRequirement, UdfArgument, UdfInputMetadata,
  UdfPayload, UDFWorkerDataFormat}

/** Engine-owned values available when initializing one external UDF worker session. */
private[sql] final case class ExternalUDFInitContext(
    protocolVersion: Int,
    dataFormat: UDFWorkerDataFormat,
    inputSchema: Array[Byte],
    outputSchema: Array[Byte],
    timezone: String,
    availableProperties: Map[String, String] = Map.empty,
    resourceDirectories: Map[String, String] = Map.empty)

/** Builds protocol initialization messages outside the Catalyst expression layer. */
private[sql] object ExternalUDFInitBuilder {

  def build(udf: ExternalUserDefinedFunction, context: ExternalUDFInitContext): Init = {
    require(context.inputSchema != null, "External UDF input schema must not be null")
    require(context.outputSchema != null, "External UDF output schema must not be null")
    require(context.timezone != null, "External UDF timezone must not be null")
    require(context.availableProperties != null, "Available properties must not be null")
    require(context.resourceDirectories != null, "Resource directories must not be null")

    val properties = requestedProperties(
      udf.sessionSpec.getStaticPropertiesMap.asScala.toMap,
      udf.sessionSpec.getPropertyRequirementsMap.asScala.toMap,
      context.availableProperties)
    val resourceDirectories = requestedResourceDirectories(
      udf.sessionSpec.getRequiredResourceDirectoriesList.asScala.toSeq,
      context.resourceDirectories)

    Init.newBuilder()
      .setProtocolVersion(context.protocolVersion)
      .setDataFormat(context.dataFormat)
      .setInputSchema(ByteString.copyFrom(context.inputSchema))
      .setOutputSchema(ByteString.copyFrom(context.outputSchema))
      .setTimezone(context.timezone)
      .setUdf(buildPayload(udf))
      .putAllProperties(properties.asJava)
      .putAllResourceDirectories(resourceDirectories.asJava)
      .build()
  }

  private def buildPayload(udf: ExternalUserDefinedFunction): UdfPayload = {
    val builder = UdfPayload.newBuilder()
      .setPayload(ByteString.copyFrom(udf.payload))
      .setFormat(udf.payloadFormat)
      .setInput(buildInputMetadata(udf.children))
    udf.name.foreach(builder.setName)
    udf.evalType.foreach(builder.setEvalType)
    builder.build()
  }

  private def buildInputMetadata(children: Seq[Expression]): UdfInputMetadata = {
    val arguments = children.map {
      case NamedArgumentExpression(argumentName, value) => (value, Some(argumentName))
      case expression => (expression, None)
    }
    val inputSchema = StructType(arguments.zipWithIndex.map {
      case ((expression, _), index) =>
        StructField(s"_$index", expression.dataType, expression.nullable)
    })
    val builder = UdfInputMetadata.newBuilder().setSchemaJson(inputSchema.json)
    arguments.zipWithIndex.foreach { case ((_, argumentName), offset) =>
      val argument = UdfArgument.newBuilder().setInputOffset(offset)
      argumentName.foreach(argument.setName)
      builder.addArguments(argument)
    }
    builder.build()
  }

  private def requestedProperties(
      staticProperties: Map[String, String],
      requirements: Map[String, PropertyRequirement],
      availableProperties: Map[String, String]): Map[String, String] = {
    val missingRequired = requirements.iterator.collect {
      case (name, requirement)
          if requirement.getIsRequired && !availableProperties.contains(name) => name
    }.toSeq.sorted
    require(
      missingRequired.isEmpty,
      s"Missing required properties: ${missingRequired.mkString(", ")}")

    val dynamicProperties = requirements.keysIterator.flatMap { name =>
      availableProperties.get(name).map { value =>
        require(value != null, s"Null property value for $name")
        name -> value
      }
    }.toMap
    staticProperties ++ dynamicProperties
  }

  private def requestedResourceDirectories(
      requiredNames: Seq[String],
      availableDirectories: Map[String, String]): Map[String, String] = {
    val missingNames = requiredNames.filterNot(availableDirectories.contains).sorted
    require(
      missingNames.isEmpty,
      s"Missing required resource directories: ${missingNames.mkString(", ")}")
    val emptyNames = requiredNames.filter { name =>
      val directory = availableDirectories(name)
      directory == null || directory.isEmpty
    }.sorted
    require(
      emptyNames.isEmpty,
      s"Empty required resource directories: ${emptyNames.mkString(", ")}")
    requiredNames.iterator.map(name => name -> availableDirectories(name)).toMap
  }
}
