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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Expression, ExternalUserDefinedFunction,
  Literal, NamedArgumentExpression, NamedExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField,
  StructType}
import org.apache.spark.udf.worker.{Init, PropertyRequirement, UDFWorkerDataFormat,
  UDFWorkerSpecification, WorkerSessionSpecification}

class ExternalUDFInitializationSuite extends SparkFunSuite {

  private def propertyRequirement(isRequired: Boolean): PropertyRequirement = {
    PropertyRequirement.newBuilder().setIsRequired(isRequired).build()
  }

  private def expectExternalUDF(expr: Expression): ExternalUserDefinedFunction = expr match {
    case udf: ExternalUserDefinedFunction => udf
    case other => fail(s"Expected ExternalUserDefinedFunction, found ${other.getClass.getName}")
  }

  private def initContext(
      availableProperties: Map[String, String] = Map.empty,
      resourceDirectories: Map[String, String] = Map.empty,
      timezone: String = "America/Los_Angeles"): ExternalUDFInitContext = {
    ExternalUDFInitContext(
      protocolVersion = 1,
      dataFormat = UDFWorkerDataFormat.ARROW,
      inputSchema = Array[Byte](1, 2),
      outputSchema = Array[Byte](3, 4),
      timezone = timezone,
      availableProperties = availableProperties,
      resourceDirectories = resourceDirectories)
  }

  private def genericFunction(
      session: WorkerSessionSpecification.Builder,
      children: Seq[Expression] = Seq(Literal(1))): ExternalUserDefinedFunction = {
    ExternalUserDefinedFunction(
      name = Some("generic"),
      workerSpec = UDFWorkerSpecification.getDefaultInstance,
      payload = Array[Byte](5, 6),
      dataType = IntegerType,
      children = children,
      udfDeterministic = true,
      udfNullable = false,
      sessionSpec = session.build())
  }

  private def buildInit(
      function: ExternalUserDefinedFunction,
      context: ExternalUDFInitContext = initContext()): Init = {
    ExternalUDFInitBuilder.build(function, context)
  }

  test("build Init from declared properties") {
    val session = WorkerSessionSpecification.newBuilder()
      .putStaticProperties("static", "static-value")
      .putPropertyRequirements("required", propertyRequirement(isRequired = true))
      .putPropertyRequirements("optional", propertyRequirement(isRequired = false))
      .addRequiredResourceDirectories("inputs")
    val function = genericFunction(session)

    val init = buildInit(function, initContext(
      availableProperties = Map(
        "required" -> "required-value",
        "unrequested" -> "not-forwarded"),
      resourceDirectories = Map(
        "inputs" -> "/var/resources/input-data",
        "unrequested" -> "/var/resources/private")))

    assert(init.getProtocolVersion === 1)
    assert(init.getDataFormat === UDFWorkerDataFormat.ARROW)
    assert(init.getInputSchema.toByteArray.sameElements(Array[Byte](1, 2)))
    assert(init.getOutputSchema.toByteArray.sameElements(Array[Byte](3, 4)))
    assert(init.getTimezone === "America/Los_Angeles")
    assert(init.getPropertiesMap.asScala.toMap === Map(
      "static" -> "static-value",
      "required" -> "required-value"))
    assert(init.getResourceDirectoriesMap.asScala.toMap ===
      Map("inputs" -> "/var/resources/input-data"))
    assert(init.getUdf.getName === "generic")
    assert(init.getUdf.getFormat === "raw-v1")
    assert(init.getUdf.getPayload.toByteArray.sameElements(Array[Byte](5, 6)))
    assert(init.getUdf.getInput.getArguments(0).getInputOffset === 0)
    assert(!init.getUdf.getInput.getArguments(0).hasName)
    assert(!init.getUdf.hasEvalType)
    assert(!init.hasParameters)
  }

  test("reject empty and conflicting property declarations when constructing the UDF") {
    val emptyName = WorkerSessionSpecification.newBuilder()
      .putPropertyRequirements("", propertyRequirement(isRequired = false))
    val emptyNameError = intercept[IllegalArgumentException] {
      genericFunction(emptyName)
    }
    assert(emptyNameError.getMessage === "requirement failed: " +
      "Worker session contains an empty property name")

    val conflicting = WorkerSessionSpecification.newBuilder()
      .putStaticProperties("duplicate", "static")
      .putPropertyRequirements("duplicate", propertyRequirement(isRequired = false))
    val conflictingError = intercept[IllegalArgumentException] {
      genericFunction(conflicting)
    }
    assert(conflictingError.getMessage === "requirement failed: " +
      "Worker session contains properties declared as both static and dynamic: duplicate")
  }

  test("enforce required properties and omit unavailable optional properties") {
    val session = WorkerSessionSpecification.newBuilder()
      .putPropertyRequirements("required.b", propertyRequirement(isRequired = true))
      .putPropertyRequirements("optional", propertyRequirement(isRequired = false))
      .putPropertyRequirements("required.a", propertyRequirement(isRequired = true))
    val function = genericFunction(session)

    val missingError = intercept[IllegalArgumentException] {
      buildInit(function, initContext(availableProperties = Map("optional" -> "present")))
    }
    assert(missingError.getMessage ===
      "requirement failed: Missing required properties: required.a, required.b")

    val init = buildInit(function, initContext(availableProperties = Map(
      "required.a" -> "",
      "required.b" -> "value",
      "unrequested" -> "not-forwarded")))
    assert(init.getPropertiesMap.asScala.toMap === Map(
      "required.a" -> "",
      "required.b" -> "value"))
  }

  test("reject a null selected property value before calling protobuf") {
    val session = WorkerSessionSpecification.newBuilder()
      .putPropertyRequirements("property", propertyRequirement(isRequired = true))
    val error = intercept[IllegalArgumentException] {
      buildInit(
        genericFunction(session),
        initContext(availableProperties = Map("property" -> null)))
    }
    assert(error.getMessage === "requirement failed: Null property value for property")
  }

  test("reject invalid resource declarations when constructing the UDF") {
    val emptyNameError = intercept[IllegalArgumentException] {
      genericFunction(WorkerSessionSpecification.newBuilder()
        .addRequiredResourceDirectories(""))
    }
    assert(emptyNameError.getMessage === "requirement failed: " +
      "Worker session contains an empty required resource directory name")

    val duplicateError = intercept[IllegalArgumentException] {
      genericFunction(WorkerSessionSpecification.newBuilder()
        .addRequiredResourceDirectories("input")
        .addRequiredResourceDirectories("input"))
    }
    assert(duplicateError.getMessage === "requirement failed: " +
      "Worker session contains duplicate required resource directories: input")
  }

  test("reject missing and empty required resource directories") {
    val function = genericFunction(WorkerSessionSpecification.newBuilder()
      .addRequiredResourceDirectories("input"))

    val missingError = intercept[IllegalArgumentException] {
      buildInit(function)
    }
    assert(missingError.getMessage ===
      "requirement failed: Missing required resource directories: input")

    Seq("", null).foreach { path =>
      val emptyError = intercept[IllegalArgumentException] {
        buildInit(function, initContext(resourceDirectories = Map("input" -> path)))
      }
      assert(emptyError.getMessage ===
        "requirement failed: Empty required resource directories: input")
    }
  }

  test("forward only declared resource directories") {
    val noResources = buildInit(genericFunction(WorkerSessionSpecification.newBuilder()),
      initContext(resourceDirectories = Map("unrequested" -> "/var/resources/private")))
    assert(noResources.getResourceDirectoriesMap.isEmpty)

    val function = genericFunction(WorkerSessionSpecification.newBuilder()
      .addRequiredResourceDirectories("input")
      .addRequiredResourceDirectories("cache"))
    val resolved = buildInit(function, initContext(resourceDirectories = Map(
      "input" -> "/var/resources/input",
      "cache" -> "/var/resources/cache",
      "unrequested" -> "/var/resources/private")))
    assert(resolved.getResourceDirectoriesMap.asScala.toMap === Map(
      "input" -> "/var/resources/input",
      "cache" -> "/var/resources/cache"))
  }

  test("rebuild logical input metadata after children are rewritten") {
    val function = genericFunction(
      WorkerSessionSpecification.newBuilder(),
      Seq(Literal(1), NamedArgumentExpression("named", Literal("value"))))
    val init = buildInit(function)

    val inputSchema = DataType.fromJson(init.getUdf.getInput.getSchemaJson)
    assert(inputSchema === StructType(Seq(
      StructField("_0", IntegerType, nullable = false),
      StructField("_1", StringType, nullable = false))))
    val arguments = init.getUdf.getInput.getArgumentsList.asScala.map { argument =>
      (argument.getInputOffset, if (argument.hasName) Some(argument.getName) else None)
    }
    assert(arguments === Seq((0, None), (1, Some("named"))))

    val rewritten = expectExternalUDF(function.withNewChildren(Seq(
      Literal(1L),
      NamedArgumentExpression("renamed", Literal("value")))))
    val rewrittenInit = buildInit(rewritten)
    val rewrittenSchema = DataType.fromJson(rewrittenInit.getUdf.getInput.getSchemaJson)
    assert(rewritten.payload eq function.payload)
    assert(rewrittenSchema === StructType(Seq(
      StructField("_0", LongType, nullable = false),
      StructField("_1", StringType, nullable = false))))
    val rewrittenArguments = rewrittenInit.getUdf.getInput.getArgumentsList.asScala.map { arg =>
      (arg.getInputOffset, if (arg.hasName) Some(arg.getName) else None)
    }
    assert(rewrittenArguments === Seq((0, None), (1, Some("renamed"))))

    val canonicalized = expectExternalUDF(function.canonicalized)
    assert(canonicalized.resultId.id === -1L)
    assert(canonicalized.payload eq function.payload)
    assert(function.semanticEquals(function.copy(resultId = NamedExpression.newExprId)))
  }
}
