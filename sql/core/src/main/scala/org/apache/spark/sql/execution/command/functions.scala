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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo


/**
 * The DDL command that creates a function.
 * alias: the class name that implements the created function.
 * resources: Jars, files, or archives which need to be added to the environment when the function
 *            is referenced for the first time by a session.
 * isTemp: indicates if it is a temporary function.
 */
// TODO: Use Seq[FunctionResource] instead of Seq[(String, String)] for resources.
case class CreateFunction(
    databaseName: Option[String],
    functionName: String,
    className: String,
    resources: Seq[(String, String)],
    isTemp: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (isTemp) {
      if (databaseName.isDefined) {
        throw new AnalysisException(
          s"It is not allowed to provide database name when defining a temporary function. " +
            s"However, database name ${databaseName.get} is provided.")
      }
      // We first load resources and then put the builder in the function registry.
      // Please note that it is allowed to overwrite an existing temp function.
      sqlContext.sessionState.catalog.loadFunctionResources(resources)
      val info = new ExpressionInfo(className, functionName)
      val builder =
        sqlContext.sessionState.catalog.makeFunctionBuilder(functionName, className)
      sqlContext.sessionState.catalog.createTempFunction(
        functionName, info, builder, ignoreIfExists = false)
    } else {
      val dbName = databaseName.getOrElse(sqlContext.sessionState.catalog.getCurrentDatabase)
      val func = FunctionIdentifier(functionName, Some(dbName))
      val catalogFunc = CatalogFunction(func, className, resources)
      // We are creating a permanent function. First, we want to check if this function
      // has already been created.
      // Check if the function to create is already existing. If so, throw exception.
      if (sqlContext.sessionState.catalog.functionExists(func)) {
        throw new AnalysisException(
          s"Function '$functionName' already exists in database '$dbName'.")
      }
      // This function will be loaded into the FunctionRegistry when a query uses it.
      sqlContext.sessionState.catalog.createFunction(catalogFunc)
    }
    Seq.empty[Row]
  }
}

/**
 * The DDL command that drops a function.
 * ifExists: returns an error if the function doesn't exist, unless this is true.
 * isTemp: indicates if it is a temporary function.
 */
case class DropFunction(
    databaseName: Option[String],
    functionName: String,
    ifExists: Boolean,
    isTemp: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (isTemp) {
      if (databaseName.isDefined) {
        throw new AnalysisException(
          s"It is not allowed to provide database name when dropping a temporary function. " +
            s"However, database name ${databaseName.get} is provided.")
      }
      sqlContext.sessionState.catalog.dropTempFunction(functionName, ifExists)
    } else {
      // We are dropping a permanent.
      val dbName = databaseName.getOrElse(sqlContext.sessionState.catalog.getCurrentDatabase)
      val func = FunctionIdentifier(functionName, Some(dbName))
      if (!ifExists) {
        if (!sqlContext.sessionState.catalog.functionExists(func)) {
          throw new AnalysisException(
            s"Function '$functionName' does not exist in database '$dbName'.")
        }
      }
      sqlContext.sessionState.catalog.dropFunction(func)
    }
    Seq.empty[Row]
  }
}
