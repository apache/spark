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

package org.apache.spark.sql.connect.utils

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.google.protobuf.{Any => ProtoAny, Message}

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{PipelineAnalysisContext, UserContext}

/**
 * Utilities for working with PipelineAnalysisContext in the user context.
 */
object PipelineAnalysisContextUtils {

  /** Get a list of extensions from the user context. */
  private def getExtensionList[T <: Message: ClassTag](
      extensions: mutable.Buffer[ProtoAny]): Seq[T] = {
    val cls = implicitly[ClassTag[T]].runtimeClass
      .asInstanceOf[Class[_ <: Message]]
    extensions.collect {
      case any if any.is(cls) => any.unpack(cls).asInstanceOf[T]
    }.toSeq
  }

  /** Get a list of PipelineAnalysisContext extensions from the user context. */
  private def getPipelineAnalysisContextList(
      userContext: UserContext): Seq[PipelineAnalysisContext] = {
    val userContextExtensions = userContext.getExtensionsList.asScala
    getExtensionList[PipelineAnalysisContext](userContextExtensions)
  }

  /** Return whether the execution / analysis is inside a pipeline flow function. */
  def hasPipelineAnalysisContext(userContext: UserContext): Boolean = {
    getPipelineAnalysisContextList(userContext).nonEmpty
  }

  /** Return whether the execution / analysis is inside a pipeline flow function. */
  def isInsidePipelineFlowFunction(userContext: UserContext): Boolean = {
    getPipelineAnalysisContextList(userContext).exists(_.hasFlowName)
  }

  /**
   * Return whether the eager execution is triggered inside pipeline flow function but the eager
   * execution type is not allowed.
   */
  def isUnsupportedEagerExecutionInsideFlowFunction(
      userContext: UserContext,
      plan: proto.Plan): Boolean = {
    // if the eager execution is not triggered inside pipeline flow function,
    // don't block it.
    if (!isInsidePipelineFlowFunction(userContext)) {
      return false
    }

    plan.getOpTypeCase match {
      // Root plan (e.g., df.collect(), df.first()) are not allowed inside flow function
      case proto.Plan.OpTypeCase.ROOT => true
      case proto.Plan.OpTypeCase.COMMAND =>
        // only spark.sql() command is allowed inside flow function.
        val commandAllowList = Set(proto.Command.CommandTypeCase.SQL_COMMAND)
        !commandAllowList.contains(plan.getCommand.getCommandTypeCase)
      // For other Plan type introduced in the future, default to block it
      case _ => true
    }
  }
}
