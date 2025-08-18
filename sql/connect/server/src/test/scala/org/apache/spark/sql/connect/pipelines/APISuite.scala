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

package org.apache.spark.sql.connect.pipelines

import org.apache.spark.sql.pipelines.graph.{PipelineUpdateContext, PipelineUpdateContextImpl, SqlGraphRegistrationContext}
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

trait SDPReference extends PipelineReference {
}

trait SDPUpdateReference extends UpdateReference {
  val updateContext: PipelineUpdateContext
}

class APISuite extends PipelineTest with APITest with SharedSparkSession {

  override def createAndRunPipeline(
    spec: TestPipelineSpec,
    sources: Seq[File]): (SDPReference, SDPUpdateReference) = {
    val sqlFiles = sources.map {
      file => TestSqlFile(
        sqlText = file.contents, sqlFilePath = file.name
      )
    }
    val graphRegistrationContext = new TestGraphRegistrationContext(
      spark,
      catalog = spec.catalog,
      database = spec.database
    )
    sqlFiles.foreach { sqlFile =>
      new SqlGraphRegistrationContext(graphRegistrationContext).processSqlFile(
        sqlText = sqlFile.sqlText,
        sqlFilePath = sqlFile.sqlFilePath,
        spark = spark
      )
    }
    val unresolvedDataflowGraph = graphRegistrationContext
      .toDataflowGraph
    val pipelineUpdateContext = new PipelineUpdateContextImpl(
      unresolvedDataflowGraph, eventCallback = _ => ())
    pipelineUpdateContext.pipelineExecution.startPipeline()


    (new SDPReference {}, new SDPUpdateReference {
      override val updateContext: PipelineUpdateContext = pipelineUpdateContext
    })
  }

  override def awaitPipelineUpdateTermination(update: UpdateReference): Unit = {
    update match {
      case ref: SDPUpdateReference =>
       ref.updateContext.pipelineExecution.awaitCompletion()
      case _ => throw new IllegalArgumentException("Invalid UpdateReference type")
    }
  }

  override def stopPipelineUpdate(update: UpdateReference): Unit = {
    update match {
      case ref: SDPUpdateReference =>
        ref.updateContext.pipelineExecution.stopPipeline()
      case _ => throw new IllegalArgumentException("Invalid UpdateReference type")
    }
  }
}



