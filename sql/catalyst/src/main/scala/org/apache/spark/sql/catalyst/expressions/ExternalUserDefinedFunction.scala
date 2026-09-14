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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_UDF, TreePattern}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType
import org.apache.spark.udf.worker.{UDFWorkerSpecification, WorkerSessionSpecification}

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
 * The session specification declares the named properties and resources needed for each worker
 * session. Physical execution combines it with engine-owned values when constructing the
 * initialization message.
 *
 * @param name Optional name of the UDF.
 * @param workerSpec Specification of the worker that executes this UDF.
 * @param payload Opaque serialized function definition.
 * @param dataType Return type of the UDF.
 * @param children Input argument expressions.
 * @param inputTypes Optional declared input types for validation.
 * @param udfDeterministic Whether this UDF is deterministic.
 * @param udfNullable Whether this UDF can return null.
 * @param resultId Unique Catalyst expression ID.
 * @param payloadFormat Format tag identifying the opaque payload encoding.
 * @param evalType Optional worker-specific dispatch hint.
 * @param sessionSpec Named properties and resources required to initialize a worker session.
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
    evalType: Option[String] = None,
    sessionSpec: WorkerSessionSpecification = WorkerSessionSpecification.getDefaultInstance)
  extends Expression with NonSQLExpression with Unevaluable {

  require(payload != null, "External UDF payload must not be null")
  require(
    payloadFormat != null && payloadFormat.nonEmpty,
    "External UDF payload format must be non-empty")
  require(sessionSpec != null, "External UDF session specification must not be null")
  ExternalUserDefinedFunction.validateSessionSpecification(sessionSpec)

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

  private def validateSessionSpecification(session: WorkerSessionSpecification): Unit = {
    val staticProperties = session.getStaticPropertiesMap.keySet.asScala.toSet
    val requiredProperties = session.getPropertyRequirementsMap.keySet.asScala.toSet
    require(
      !(staticProperties ++ requiredProperties).contains(""),
      "Worker session contains an empty property name")

    val duplicateProperties = staticProperties.intersect(requiredProperties).toSeq.sorted
    require(
      duplicateProperties.isEmpty,
      s"Worker session contains properties declared as both static and dynamic: " +
        duplicateProperties.mkString(", "))

    val resourceDirectories = session.getRequiredResourceDirectoriesList.asScala.toSeq
    require(
      !resourceDirectories.contains(""),
      "Worker session contains an empty required resource directory name")
    val duplicateResources = resourceDirectories.groupBy(identity)
      .collect { case (name, occurrences) if occurrences.size > 1 => name }
      .toSeq
      .sorted
    require(
      duplicateResources.isEmpty,
      s"Worker session contains duplicate required resource directories: " +
        duplicateResources.mkString(", "))
  }
}
