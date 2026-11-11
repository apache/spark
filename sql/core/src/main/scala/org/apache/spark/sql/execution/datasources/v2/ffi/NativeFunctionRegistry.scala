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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, URI}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId, ImplicitCastInputTypes, NamedArgumentExpression, NamedExpression, Unevaluable, UserDefinedExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.{NATIVE_UDF, TreePattern}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.util.Utils

/**
 * Registers native functions: `CREATE TEMPORARY FUNCTION f AS 'name' USING PACKAGE 'pkg'` makes
 * `f` call the function `name` of the native package `pkg`, with the types that the manifest of
 * the package gives, so that the driver does not load the library of the package.
 */
object NativeFunctionRegistry extends Logging {

  // The packages of USING PACKAGE that are fetched from a URI, each into a directory of its own.
  private lazy val fetchDir: File = Utils.createTempDir(namePrefix = "native-function-packages")

  /**
   * Returns the builder of the calls of the function `name` of the native package `pkg`, once it
   * checked that Spark can run the function. Function names are case-insensitive, as data source
   * names are.
   *
   * @param identifier the function as the session registers it
   * @param pkg the package: the file name of a package that the session added with
   *            `spark.addArtifact` or of a package under `spark.sql.dataSource.native.paths`,
   *            or else the path or the URI of a package
   */
  def functionBuilder(
      session: SparkSession,
      identifier: FunctionIdentifier,
      name: String,
      pkg: String): Seq[Expression] => Expression = {
    if (!session.sessionState.conf.getConf(StaticSQLConf.NATIVE_UDF_ENABLED)) {
      throw QueryCompilationErrors.nativeUdfDisabledError(
        identifier, StaticSQLConf.NATIVE_UDF_ENABLED.key)
    }
    if (!NativeLibrary.isSupported) {
      throw QueryExecutionErrors.nativeUdfUnsupportedJavaVersionError(
        NativeLibrary.MINIMUM_JAVA_VERSION, Runtime.version().feature())
    }
    val found = NativeDataSourcePackage.read(
      findPackage(session, pkg).getOrElse {
        throw QueryCompilationErrors.nativeUdfPackageNotFoundError(identifier, pkg)
      })
    if (!NativeDataSourcePackage.isUdfSupported(found.manifest)) {
      throw NativeDataSourcePackage.unsupportedUdfAbiVersionError(found.path, found.manifest)
    }
    if (!found.manifest.libraries.contains(NativePlatform.current)) {
      throw NativeDataSourcePackage.unsupportedPlatformError(found.path, found.manifest)
    }
    val function = found.manifest.functions.find(_.name.equalsIgnoreCase(name)) match {
      case Some(spec: NativeFunctionSpec) => spec
      case Some(UnsupportedNativeFunction(functionName, kind)) =>
        throw QueryCompilationErrors.nativeUdfUnsupportedKindError(
          found.fileName, functionName, kind, NativeFunctionSpec.supportedKinds)
      case None =>
        throw QueryCompilationErrors.nativeUdfNotFoundError(
          found.fileName, name, found.manifest.functions.map(_.name))
    }
    val callName = identifier.unquotedString
    arguments => {
      if (arguments.exists(_.isInstanceOf[NamedArgumentExpression])) {
        throw QueryCompilationErrors.namedArgumentsNotSupported(callName)
      }
      if (arguments.length != function.arguments.length) {
        throw QueryCompilationErrors.wrongNumArgsError(
          callName, Seq(function.arguments.length), arguments.length)
      }
      NativeUDF(callName, function, found, arguments)
    }
  }

  /** Finds the file of the package of USING PACKAGE, see [[functionBuilder]]. */
  private def findPackage(session: SparkSession, pkg: String): Option[File] = {
    val isFileName = !pkg.contains('/') && !pkg.contains(File.separatorChar)
    def named(files: => Seq[File]): Option[File] =
      if (isFileName) files.find(_.getName == pkg) else None
    named(session.artifactManager.getNativeDataSourcePackages._1)
      .orElse(named(NativeDataSourceRegistry.configuredPackageFiles(session.sessionState.conf)))
      .orElse {
        val uri = Utils.resolveURI(pkg)
        if (uri.getScheme == null || uri.getScheme == "file") {
          Some(new File(uri.getPath)).filter(_.isFile)
        } else {
          logInfo(log"Fetching the native package ${MDC(URI, uri)}.")
          val file = Utils.fetchFile(uri.toString, Utils.createTempDir(fetchDir.getPath),
            session.sparkContext.conf, session.sessionState.newHadoopConf(),
            System.currentTimeMillis(), useCache = false, shouldUntar = false)
          logInfo(log"Fetched the native package ${MDC(PATH, file)}.")
          Some(file)
        }
      }
  }
}

/**
 * A call of a native function, which the driver never evaluates: [[ExtractNativeUDFs]] plans it
 * for [[EvalNativeUDFsExec]]. The analyzer casts the arguments to the types of the manifest.
 *
 * @param name the function as the query calls it
 * @param function the function in the manifest of the package
 * @param pkg the package, as the driver read it
 * @param resultId the ID of the result of the call, which tells apart the calls of a
 *                 non-deterministic function
 */
case class NativeUDF(
    name: String,
    function: NativeFunctionSpec,
    pkg: NativeDataSourcePackage,
    children: Seq[Expression],
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with ImplicitCastInputTypes with UserDefinedExpression with Unevaluable {

  override def dataType: DataType = function.returnType

  override def nullable: Boolean = true

  // As for other UDFs, so that the optimizer does not evaluate a call twice to push a filter down.
  override def expensive: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = function.arguments

  override lazy val deterministic: Boolean =
    function.deterministic && children.forall(_.deterministic)

  override def prettyName: String = name

  override def toString: String = s"$name(${children.mkString(", ")})#${resultId.id}$typeSuffix"

  override lazy val canonicalized: Expression =
    copy(children = children.map(_.canonicalized), resultId = ExprId(-1))

  final override val nodePatterns: Seq[TreePattern] = Seq(NATIVE_UDF)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): NativeUDF =
    copy(children = newChildren)
}
