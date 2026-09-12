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

package org.apache.spark.sql.catalyst.expressions

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_UDF, TreePattern}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.udf.worker.{DynamicConfigRequirement, Init, UdfArgument, UdfInputMetadata,
  UdfPayload, UDFWorkerDataFormat, UDFWorkerSpecification, WorkerContextReference}

/**
 * Language-neutral inputs used by an [[ExternalUserDefinedFunction]] to initialize one worker
 * session. The physical operator owns these engine-side values. [[ExternalUserDefinedFunction]]
 * combines them with the context declaration in its worker specification.
 *
 * The environment, dynamic configuration, and resource-directory maps may contain only
 * engine-authorized, resolved values. [[ExternalUserDefinedFunction]] selects the names requested
 * by the worker specification. In particular, callers must not expose the engine's complete
 * process environment through [[environmentVariables]]. Callers also resolve engine/task state,
 * including the task-context keys and resource directories required by a worker; the generic
 * builder only validates and forwards those values. Physical execution must populate
 * [[ExternalUDFInitContext.DRIVER_ID_CONTEXT_KEY]] and
 * [[ExternalUDFInitContext.IS_DRIVER_CONTEXT_KEY]] for every session. The driver ID defaults to
 * `SparkContext.DRIVER_IDENTIFIER`; the driver-role flag is derived from the local `SparkEnv`.
 */
@Experimental
case class ExternalUDFInitContext(
    protocolVersion: Int,
    dataFormat: UDFWorkerDataFormat,
    inputSchema: Array[Byte],
    outputSchema: Array[Byte],
    timezone: String,
    taskContext: Map[String, String] = Map.empty,
    environmentVariables: Map[String, String] = Map.empty,
    dynamicConfig: Map[String, String] = Map.empty,
    resourceDirectories: Map[String, String] = Map.empty) {

  private[expressions] def newInitBuilder(): Init.Builder = {
    Init.newBuilder()
      .setProtocolVersion(protocolVersion)
      .setDataFormat(dataFormat)
      .setInputSchema(ByteString.copyFrom(inputSchema))
      .setOutputSchema(ByteString.copyFrom(outputSchema))
      .setTimezone(timezone)
      .putAllTaskContext(taskContext.asJava)
  }
}

object ExternalUDFInitContext {
  val DRIVER_ID_CONTEXT_KEY: String = "driverId"
  val IS_DRIVER_CONTEXT_KEY: String = "isDriver"
}

/**
 * :: Experimental ::
 * A serialized external UDF that is executed in an external worker process
 * via the language-agnostic UDF worker framework.
 *
 * This is a Catalyst expression analogous to [[PythonUDF]] but
 * language-agnostic. The [[payload]] carries an opaque serialized
 * function definition whose interpretation is left to the worker.
 * The optional [[inputTypes]] declare the expected argument types for
 * validation during analysis; when absent, any input types are accepted.
 *
 * This expression is [[Unevaluable]] and requires a dedicated physical
 * operator (e.g. [[org.apache.spark.sql.execution.externalUDF.MapPartitionsExternalUDFExec]])
 * to execute.
 *
 * The worker specification declares the engine context needed for each worker session. This
 * expression supplies the invocation-specific payload metadata. Together they are sufficient to
 * construct [[Init]] without a language-specific expression subtype.
 *
 * @param name Optional name of the UDF.
 * @param workerSpec Specification of the worker that executes this UDF.
 * @param payload Opaque serialized function definition.
 * @param dataType Return type of the UDF.
 * @param children Input argument expressions.
 * @param inputTypes Optional declared input types for validation.
 * @param udfDeterministic Whether this UDF is deterministic.
 * @param udfNullable Whether this UDF can return null.
 * @param resultId Unique expression ID for this invocation.
 * @param payloadFormat Format tag identifying the opaque payload encoding.
 * @param evalType Optional worker-specific dispatch hint.
 */
@Experimental
case class ExternalUserDefinedFunction(
    name: Option[String],
    workerSpec: UDFWorkerSpecification,
    payload: Array[Byte],
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Option[Seq[DataType]] = None,
    udfDeterministic: Boolean,
    udfNullable: Boolean,
    resultId: ExprId = NamedExpression.newExprId,
    payloadFormat: String = ExternalUserDefinedFunction.DEFAULT_PAYLOAD_FORMAT,
    evalType: Option[String] = None)
  extends Expression with NonSQLExpression with Unevaluable {

  require(payloadFormat.nonEmpty, "External UDF payload format must be non-empty")

  /** Builds the complete initialization message for one worker session. */
  def buildInit(context: ExternalUDFInitContext): Init = {
    val udfBuilder = UdfPayload.newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setFormat(payloadFormat)
      .setInvocationId(resultId.id)
    name.foreach(udfBuilder.setName)
    evalType.foreach(udfBuilder.setEvalType)

    val arguments = children.map {
      case NamedArgumentExpression(argumentName, value) => (value, Some(argumentName))
      case expression => (expression, None)
    }
    val inputSchema = StructType(arguments.zipWithIndex.map {
      case ((expression, _), index) =>
        StructField(s"_$index", expression.dataType, expression.nullable)
    })
    val inputBuilder = UdfInputMetadata.newBuilder()
      .setSchemaFormat(ExternalUserDefinedFunction.INPUT_SCHEMA_FORMAT)
      .setSchema(ByteString.copyFromUtf8(inputSchema.json))
    arguments.zipWithIndex.foreach { case ((_, argumentName), offset) =>
      val argumentBuilder = UdfArgument.newBuilder().setInputOffset(offset)
      argumentName.foreach(argumentBuilder.setName)
      inputBuilder.addArguments(argumentBuilder)
    }
    udfBuilder.setInput(inputBuilder)

    val session = workerSpec.getSession
    val environmentVariables = requestedValues(
      session.getEnvironmentVariableReferencesMap.asScala.toMap,
      context.environmentVariables)
    val staticConfig = session.getStaticConfigMap.asScala.toMap
    val dynamicConfigRequirements = session.getDynamicConfigMap.asScala.toMap
    validateUniqueKeys(
      staticConfig.keySet,
      dynamicConfigRequirements.keySet)
    val dynamicConfig = requestedDynamicConfig(
      dynamicConfigRequirements,
      context.dynamicConfig)
    val resourceDirectories = requestedResourceDirectories(
      session.getRequiredResourceDirectoriesList.asScala.toSeq,
      context.resourceDirectories)

    val initBuilder = context.newInitBuilder()
      .setUdf(udfBuilder)
      .putAllEnvironmentVariables(environmentVariables.asJava)
      .putAllSessionConf((staticConfig ++ dynamicConfig).asJava)
      .putAllResourceDirectories(resourceDirectories.asJava)
    initBuilder.build()
  }

  private def requestedDynamicConfig(
      requirements: Map[String, DynamicConfigRequirement],
      values: Map[String, String]): Map[String, String] = {
    val missingRequired = requirements.iterator.collect {
      case (name, requirement) if requirement.getIsRequired && !values.contains(name) => name
    }.toSeq.sorted
    require(
      missingRequired.isEmpty,
      s"Missing required dynamic configuration: ${missingRequired.mkString(", ")}")
    requirements.keysIterator.flatMap { name =>
      values.get(name).map { value =>
        require(value != null, s"Null dynamic configuration value for $name")
        name -> value
      }
    }.toMap
  }

  private def requestedResourceDirectories(
      requiredNames: Seq[String],
      values: Map[String, String]): Map[String, String] = {
    require(
      !requiredNames.contains(""),
      "Worker session contains an empty required resource directory name")
    val duplicateNames = requiredNames.groupBy(identity)
      .collect { case (name, occurrences) if occurrences.size > 1 => name }
      .toSeq
      .sorted
    require(
      duplicateNames.isEmpty,
      s"Worker session contains duplicate required resource directories: " +
        duplicateNames.mkString(", "))
    val missingNames = requiredNames.filterNot(values.contains).sorted
    require(
      missingNames.isEmpty,
      s"Missing required resource directories: ${missingNames.mkString(", ")}")
    val emptyNames = requiredNames.filter { name =>
      val directory = values(name)
      directory == null || directory.isEmpty
    }.sorted
    require(
      emptyNames.isEmpty,
      s"Empty required resource directories: ${emptyNames.mkString(", ")}")
    requiredNames.iterator.map(name => name -> values(name)).toMap
  }

  private def requestedValues(
      references: Map[String, WorkerContextReference],
      values: Map[String, String]): Map[String, String] = {
    references.iterator.flatMap { case (target, reference) =>
      require(target.nonEmpty, "Empty worker context target")
      require(reference.getSource.nonEmpty, s"Empty worker context source for $target")
      values.get(reference.getSource)
        .orElse(if (reference.hasDefaultValue) Some(reference.getDefaultValue) else None)
        .map(target -> _)
    }.toMap
  }

  private def validateUniqueKeys(keySets: Set[String]*): Unit = {
    require(!keySets.exists(_.contains("")), "Worker session context contains an empty target key")
    val duplicateKeys = keySets.iterator.flatten.toSeq
      .groupBy(identity)
      .collect { case (key, occurrences) if occurrences.size > 1 => key }
      .toSeq
      .sorted
    require(
      duplicateKeys.isEmpty,
      s"Worker session context contains duplicate target keys: ${duplicateKeys.mkString(", ")}")
  }

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = udfNullable

  override def checkInputDataTypes(): TypeCheckResult = {
    inputTypes match {
      case Some(types) if types.length != children.length =>
        throw QueryCompilationErrors.wrongNumArgsError(
          name = name.getOrElse(prettyName),
          validParametersCount = Seq(types.length),
          actualNumber = children.length)
      case Some(types) =>
        ExpectsInputTypes.checkInputDataTypes(children, types)
      case None => TypeCheckSuccess
    }
  }

  // Worker specifications and payloads can contain sensitive execution details.
  override def toString: String = {
    s"${name.getOrElse(prettyName)}(${children.mkString(", ")})#${resultId.id}$typeSuffix"
  }

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in ExternalUserDefinedFunction,
    // as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_UDF)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ExternalUserDefinedFunction =
    copy(children = newChildren)
}

object ExternalUserDefinedFunction {
  val DEFAULT_PAYLOAD_FORMAT: String = "raw-v1"
  val INPUT_SCHEMA_FORMAT: String = "spark-sql-data-type-json-v1"
}
