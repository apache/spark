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

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Expression, ExternalUDFInitContext,
  ExternalUserDefinedFunction, Literal, NamedArgumentExpression, NamedExpression, PythonUDF}
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField,
  StructType}
import org.apache.spark.udf.worker.{DynamicConfigRequirement, Init, UDFWorkerDataFormat,
  UDFWorkerSpecification, WorkerContextReference, WorkerSessionSpecification}

class ExternalUDFInitializationSuite extends QueryTest with SharedSparkSession {

  private def contextReference(
      source: String,
      defaultValue: Option[String] = None): WorkerContextReference = {
    val builder = WorkerContextReference.newBuilder().setSource(source)
    defaultValue.foreach(builder.setDefaultValue)
    builder.build()
  }

  private def dynamicConfigRequirement(isRequired: Boolean): DynamicConfigRequirement = {
    DynamicConfigRequirement.newBuilder().setIsRequired(isRequired).build()
  }

  private def expectExternalUDF(expr: Expression): ExternalUserDefinedFunction = expr match {
    case udf: ExternalUserDefinedFunction => udf
    case other => fail(s"Expected ExternalUserDefinedFunction, found ${other.getClass.getName}")
  }

  private def initContext(
      taskContext: Map[String, String] = Map(
        "partitionId" -> "3",
        ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY -> "driver",
        ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY -> "true"),
      environmentVariables: Map[String, String] = Map.empty,
      dynamicConfig: Map[String, String] = Map.empty,
      resourceDirectories: Map[String, String] = Map(
        PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY ->
          "/var/resources/artifact-1"),
      timezone: String = "America/Los_Angeles"): ExternalUDFInitContext = {
    ExternalUDFInitContext(
      protocolVersion = 1,
      dataFormat = UDFWorkerDataFormat.ARROW,
      inputSchema = Array[Byte](1, 2),
      outputSchema = Array[Byte](3, 4),
      timezone = timezone,
      taskContext = taskContext,
      environmentVariables = environmentVariables,
      dynamicConfig = dynamicConfig,
      resourceDirectories = resourceDirectories)
  }

  private def pythonFunction(
      command: Array[Byte] = Array[Byte](10, 20, 30),
      pythonIncludes: Seq[String] = Seq("first.zip", "second.zip"),
      pythonVersion: String = "3.12"): SimplePythonFunction = {
    new SimplePythonFunction(
      command = command,
      envVars = Map.empty[String, String].asJava,
      pythonIncludes = pythonIncludes.asJava,
      pythonExec = "python3",
      pythonVer = pythonVersion,
      broadcastVars = null,
      accumulator = null)
  }

  private def pythonUDF(
      children: Seq[Expression],
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF): PythonUDF = {
    PythonUDF(
      name = "plus_one",
      func = pythonFunction(),
      dataType = IntegerType,
      children = children,
      evalType = evalType,
      udfDeterministic = true)
  }

  private def genericFunction(
      session: WorkerSessionSpecification.Builder): ExternalUserDefinedFunction = {
    ExternalUserDefinedFunction(
      name = Some("generic"),
      workerSpec = UDFWorkerSpecification.newBuilder().setSession(session).build(),
      payload = Array[Byte](5, 6),
      dataType = IntegerType,
      children = Seq(Literal(1)),
      udfDeterministic = true,
      udfNullable = false)
  }

  private def pythonDynamicConfig: Map[String, String] = {
    ArrowPythonRunner.getPythonRunnerConfMap(spark.sessionState.conf) -
      SQLConf.SESSION_LOCAL_TIMEZONE.key
  }

  private def hexBytes(value: String): Array[Byte] = {
    value.grouped(2).map(pair => Integer.parseInt(pair, 16).toByte).toArray
  }

  test("generic Init resolves only worker-declared context") {
    val session = WorkerSessionSpecification.newBuilder()
      .putStaticConfig("static", "static-value")
      .putEnvironmentVariableReferences("forwarded.env", contextReference("SOURCE_ENV"))
      .putEnvironmentVariableReferences(
        "defaulted.env",
        contextReference("MISSING_ENV", Some("default-value")))
      .putEnvironmentVariableReferences("optional.env", contextReference("MISSING_OPTIONAL_ENV"))
      .putDynamicConfig("forwarded.dynamic", dynamicConfigRequirement(isRequired = true))
      .putDynamicConfig("optional.dynamic", dynamicConfigRequirement(isRequired = false))
      .addRequiredResourceDirectories("inputs")
    val function = genericFunction(session)

    val init = function.buildInit(initContext(
      environmentVariables = Map("SOURCE_ENV" -> "env-value", "SECRET" -> "not-forwarded"),
      dynamicConfig = Map(
        "forwarded.dynamic" -> "dynamic-value",
        "unrequested.dynamic" -> "not-forwarded"),
      resourceDirectories = Map(
        "inputs" -> "/var/resources/input-data",
        "unrequested" -> "/var/resources/private")))

    assert(init.getProtocolVersion === 1)
    assert(init.getDataFormat === UDFWorkerDataFormat.ARROW)
    assert(init.getInputSchema.toByteArray.sameElements(Array[Byte](1, 2)))
    assert(init.getOutputSchema.toByteArray.sameElements(Array[Byte](3, 4)))
    assert(init.getTimezone === "America/Los_Angeles")
    assert(init.getTaskContextMap.asScala.toMap === Map(
      "partitionId" -> "3",
      ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY -> "driver",
      ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY -> "true"))
    assert(init.getEnvironmentVariablesMap.asScala.toMap ===
      Map("forwarded.env" -> "env-value", "defaulted.env" -> "default-value"))
    assert(init.getSessionConfMap.asScala.toMap === Map(
      "static" -> "static-value",
      "forwarded.dynamic" -> "dynamic-value"))
    assert(init.getResourceDirectoriesMap.asScala.toMap ===
      Map("inputs" -> "/var/resources/input-data"))
    assert(init.getUdf.getName === "generic")
    assert(init.getUdf.getFormat === "raw-v1")
    assert(init.getUdf.getInvocationId === function.resultId.id)
    assert(init.getUdf.getPayload.toByteArray.sameElements(Array[Byte](5, 6)))
    assert(init.getUdf.getInput.getSchemaFormat === "spark-sql-data-type-json-v1")
    assert(init.getUdf.getInput.getArguments(0).getInputOffset === 0)
    assert(!init.getUdf.getInput.getArguments(0).hasName)
    assert(!init.getUdf.hasEvalType)
    assert(!init.hasParameters)
  }

  test("generic Init rejects conflicting session context targets") {
    val session = WorkerSessionSpecification.newBuilder()
      .putStaticConfig("duplicate", "static")
      .putDynamicConfig("duplicate", dynamicConfigRequirement(isRequired = false))
    val function = genericFunction(session)

    val error = intercept[IllegalArgumentException] {
      function.buildInit(initContext())
    }
    assert(error.getMessage.contains("duplicate target keys: duplicate"))
  }

  test("generic Init rejects empty worker context references") {
    val session = WorkerSessionSpecification.newBuilder()
      .putEnvironmentVariableReferences("target", contextReference(""))
    val function = genericFunction(session)

    val error = intercept[IllegalArgumentException] {
      function.buildInit(initContext())
    }
    assert(error.getMessage.contains("Empty worker context source for target"))

    val emptyConfigName = WorkerSessionSpecification.newBuilder()
      .putDynamicConfig("", dynamicConfigRequirement(isRequired = false))
    val emptyNameError = intercept[IllegalArgumentException] {
      genericFunction(emptyConfigName).buildInit(initContext())
    }
    assert(emptyNameError.getMessage.contains("empty target key"))
  }

  test("generic Init enforces dynamic configuration requirements") {
    val session = WorkerSessionSpecification.newBuilder()
      .putDynamicConfig("required.b", dynamicConfigRequirement(isRequired = true))
      .putDynamicConfig("optional", dynamicConfigRequirement(isRequired = false))
      .putDynamicConfig("required.a", dynamicConfigRequirement(isRequired = true))
    val function = genericFunction(session)

    val error = intercept[IllegalArgumentException] {
      function.buildInit(initContext(dynamicConfig = Map("optional" -> "present")))
    }
    assert(error.getMessage.contains(
      "Missing required dynamic configuration: required.a, required.b"))

    val init = function.buildInit(initContext(dynamicConfig = Map(
      "required.a" -> "",
      "required.b" -> "value",
      "optional" -> "optional-value",
      "unrequested" -> "not-forwarded")))
    assert(init.getSessionConfMap.asScala.toMap === Map(
      "required.a" -> "",
      "required.b" -> "value",
      "optional" -> "optional-value"))
  }

  test("generic Init validates required resource directories") {
    def build(requiredNames: String*)(resources: Map[String, String]): Init = {
      val session = WorkerSessionSpecification.newBuilder()
        .addAllRequiredResourceDirectories(requiredNames.asJava)
      genericFunction(session).buildInit(initContext(resourceDirectories = resources))
    }

    val emptyName = intercept[IllegalArgumentException] {
      build("")(Map.empty)
    }
    assert(emptyName.getMessage.contains("empty required resource directory name"))

    val duplicateNames = intercept[IllegalArgumentException] {
      build("input", "input")(Map("input" -> "/var/resources/input"))
    }
    assert(duplicateNames.getMessage.contains(
      "duplicate required resource directories: input"))

    val missing = intercept[IllegalArgumentException] {
      build("input")(Map.empty)
    }
    assert(missing.getMessage.contains("Missing required resource directories: input"))

    val emptyPath = intercept[IllegalArgumentException] {
      build("input")(Map("input" -> ""))
    }
    assert(emptyPath.getMessage.contains("Empty required resource directories: input"))

    assert(build()(Map("unrequested" -> "/var/resources/private"))
      .getResourceDirectoriesMap.isEmpty)
    val resolved = build("input", "cache")(Map(
      "input" -> "/var/resources/input",
      "cache" -> "/var/resources/cache",
      "unrequested" -> "/var/resources/private"))
    assert(resolved.getResourceDirectoriesMap.asScala.toMap === Map(
      "input" -> "/var/resources/input",
      "cache" -> "/var/resources/cache"))
  }

  test("PySpark conversion uses generic Init and refreshes rewritten input metadata") {
    val udf = pythonUDF(Seq(
      Literal(1),
      NamedArgumentExpression("named", Literal("value"))))
    val workerSpec = PythonUDFWorkerSpecBuilder.build(
      udf.func,
      spark.sparkContext.getConf)
    val external = PythonExternalUDFAdapter.toExternalUDF(
      udf,
      workerSpec)
    val pythonTaskContext = Map(
      "partitionId" -> "3",
      ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY -> "driver",
      ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY -> "false")
    val currentDynamicConfig =
      pythonDynamicConfig.updated(SQLConf.PYSPARK_BINARY_AS_BYTES.key, "false")
    val init = external.buildInit(initContext(
      taskContext = pythonTaskContext,
      environmentVariables = Map("UNREQUESTED_SECRET" -> "not-forwarded"),
      dynamicConfig = currentDynamicConfig))

    assert(external.getClass === classOf[ExternalUserDefinedFunction])
    assert(init.getUdf.getName === "plus_one")
    assert(init.getUdf.getFormat === "pyspark-udf-experimental")
    assert(init.getUdf.getEvalType === PythonEvalType.SQL_ARROW_BATCHED_UDF.toString)
    assert(init.getUdf.getInvocationId === udf.resultId.id)
    assert(init.getEnvironmentVariablesMap.isEmpty)
    assert(init.getTaskContextMap.get(
      ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY) === "driver")
    assert(init.getTaskContextMap.get(
      ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY) === "false")
    assert(init.getSessionConfMap.get(SQLConf.PYSPARK_BINARY_AS_BYTES.key) === "false")
    assert((pythonDynamicConfig - SQLConf.PYSPARK_BINARY_AS_BYTES.key).forall { case (key, value) =>
      init.getSessionConfMap.get(key) == value
    })
    assert(!init.getSessionConfMap.containsKey(SQLConf.SESSION_LOCAL_TIMEZONE.key))
    assert(!init.hasParameters)

    val nextTaskContext = pythonTaskContext
      .updated("partitionId", "4")
      .updated(ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY, "true")
    val nextInit = external.buildInit(initContext(
      taskContext = nextTaskContext,
      dynamicConfig = pythonDynamicConfig,
      resourceDirectories = Map(
        PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY ->
          "/var/resources/artifact-2"),
      timezone = "UTC"))
    assert(nextInit.getUdf === init.getUdf)
    assert(nextInit.getTaskContextMap.get("partitionId") === "4")
    assert(nextInit.getTaskContextMap.get(
      ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY) === "driver")
    assert(nextInit.getTaskContextMap.get(
      ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY) === "true")
    assert(nextInit.getResourceDirectoriesMap.get(
      PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY) ===
      "/var/resources/artifact-2")
    assert(nextInit.getTimezone === "UTC")

    val decoded = PythonUDFPayload.decode(external.payload)
    assert(decoded.command.sameElements(Array[Byte](10, 20, 30)))
    assert(decoded.pythonIncludes === Vector("first.zip", "second.zip"))
    assert(decoded.pythonVersion === "3.12")

    val inputSchema = DataType.fromJson(init.getUdf.getInput.getSchema.toStringUtf8)
    assert(inputSchema === StructType(Seq(
      StructField("_0", IntegerType, nullable = false),
      StructField("_1", StringType, nullable = false))))
    assert(init.getUdf.getInput.getArgumentsList.asScala.map(_.getInputOffset) === Seq(0, 1))
    assert(!init.getUdf.getInput.getArguments(0).hasName)
    assert(init.getUdf.getInput.getArguments(1).getName === "named")

    val rewritten = expectExternalUDF(external.withNewChildren(Seq(
      Literal(1L),
      NamedArgumentExpression("renamed", Literal("value")))))
    val rewrittenInit = rewritten.buildInit(initContext(dynamicConfig = pythonDynamicConfig))
    val rewrittenSchema =
      DataType.fromJson(rewrittenInit.getUdf.getInput.getSchema.toStringUtf8)
    assert(rewritten.payload eq external.payload)
    assert(rewrittenInit.getUdf.getInvocationId === init.getUdf.getInvocationId)
    assert(rewrittenInit.getUdf.getInput.getSchemaFormat === "spark-sql-data-type-json-v1")
    assert(rewrittenSchema === StructType(Seq(
      StructField("_0", LongType, nullable = false),
      StructField("_1", StringType, nullable = false))))
    val rewrittenArguments = rewrittenInit.getUdf.getInput.getArgumentsList.asScala.map { arg =>
      (arg.getInputOffset, if (arg.hasName) Some(arg.getName) else None)
    }
    assert(rewrittenArguments === Seq((0, None), (1, Some("renamed"))))

    val canonicalized = expectExternalUDF(external.canonicalized)
    assert(canonicalized.resultId.id === -1L)
    assert(canonicalized.payload eq external.payload)
    assert(external.semanticEquals(external.copy(resultId = NamedExpression.newExprId)))
  }

  test("Python worker specification declares its generic session requirements") {
    val spec = PythonUDFWorkerSpecBuilder.build(
      pythonFunction(),
      spark.sparkContext.getConf)
    val session = spec.getSession
    val dynamicConfig = session.getDynamicConfigMap.asScala.toMap

    val expectedDynamicKeys = ArrowPythonRunner.getPythonRunnerConfEntries
      .map(_.key)
      .filterNot(_ == SQLConf.SESSION_LOCAL_TIMEZONE.key)
      .toSet
    assert(dynamicConfig.keySet === expectedDynamicKeys)
    val optionalKeys = Set(
      SQLConf.PYTHON_UDF_ARROW_CONCURRENCY_LEVEL.key,
      SQLConf.PYTHON_UDF_PROFILER.key,
      SQLConf.PYTHON_DATA_SOURCE_PROFILER.key)
    assert(dynamicConfig.forall { case (name, requirement) =>
      requirement.getIsRequired === !optionalKeys.contains(name)
    })
    assert(session.getRequiredResourceDirectoriesList.asScala.toSeq ===
      Seq(PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY))
    assert(session.getStaticConfigMap.isEmpty)
    val defaultFunction = PythonExternalUDFAdapter.toExternalUDF(
      pythonUDF(Seq(Literal(1))),
      spec)
    val defaultInit = defaultFunction.buildInit(initContext(
      dynamicConfig = pythonDynamicConfig,
      resourceDirectories = Map(
        PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY ->
          "/var/resources/artifact-default")))
    assert(defaultInit.getSessionConfMap.get(SQLConf.PYSPARK_BINARY_AS_BYTES.key) === "true")
    assert(!defaultInit.getSessionConfMap.containsKey(
      SQLConf.PYTHON_UDF_ARROW_CONCURRENCY_LEVEL.key))
    assert(defaultInit.getResourceDirectoriesMap.get(
      PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY) ===
      "/var/resources/artifact-default")

    val missingConfig = intercept[IllegalArgumentException] {
      defaultFunction.buildInit(initContext())
    }
    assert(missingConfig.getMessage.contains("Missing required dynamic configuration"))

    Seq(
      Map.empty[String, String],
      Map(PythonUDFWorkerSpecBuilder.ARTIFACTS_RESOURCE_DIRECTORY -> "")
    ).foreach { resourceDirectories =>
      val error = intercept[IllegalArgumentException] {
        defaultFunction.buildInit(initContext(
          dynamicConfig = pythonDynamicConfig,
          resourceDirectories = resourceDirectories))
      }
      assert(error.getMessage.contains("resource director"))
    }
    assert(session.getEnvironmentVariableReferencesMap.isEmpty)
  }

  test("PySpark conversion rejects unsupported evaluation types") {
    val udf = pythonUDF(Seq(Literal(1)), PythonEvalType.SQL_BATCHED_UDF)
    val error = intercept[IllegalArgumentException] {
      PythonExternalUDFAdapter.toExternalUDF(
        udf,
        UDFWorkerSpecification.getDefaultInstance)
    }
    assert(error.getMessage.contains("Unsupported Python external UDF eval type"))
  }

  test("experimental Python payload encoding has an explicit wire version") {
    val payload = PythonUDFPayload.encode(pythonFunction(
      command = Array[Byte](1),
      pythonIncludes = Seq("x"),
      pythonVersion = "v"))
    val expectedPayload = hexBytes(
      "505955440000000100000001010000000100000001780000000176")
    assert(payload.sameElements(expectedPayload))

    Seq(0, 2).foreach { version =>
      val unsupportedVersion = payload.clone()
      ByteBuffer.wrap(unsupportedVersion).putInt(Integer.BYTES, version)
      assert(intercept[IllegalArgumentException] {
        PythonUDFPayload.decode(unsupportedVersion)
      }.getMessage.contains(s"unsupported version $version"))
    }
    assert(intercept[IllegalArgumentException] {
      PythonUDFPayload.decode(payload :+ 0.toByte)
    }.getMessage.contains("trailing bytes"))
    assert(intercept[IllegalArgumentException] {
      PythonUDFPayload.decode(payload.dropRight(1))
    }.getMessage.contains("invalid Python version length"))
  }
}
