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

package org.apache.spark.internal.io

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * Package object to assist in switching to the Hadoop Hadoop 3
 * [[org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory]] factory
 * mechanism for dynamically loading committers for the destination stores.
 *
 * = Using Alternative Committers with Spark and Hadoop 3 =
 *
 * Hadoop 3.1 adds a means to select a different output committer when writing
 * data to object stores. This can provide higher performance as well as
 * addressing the consistency and atomicity problems encountered on some filesystems.
 *
 * Every object store can implement its own committer factory: the factory
 * itself will then instantiated the committer of its choice.
 *
 * == Prerequisites ==
 *
 * Apache Hadoop 3.0.2 or later for the factory APIs, for the S3A connectors, Hadoop 3.1+
 *
 * The Hadoop cluster needs to be configured for the binding from filesystem scheme
 * to factory. In Hadoop 3.1 this is done automatically for s3a in the file
 * `mapred-default.xml`.
 * Other stores' committers may need to be explicitly declared.
 *
 * {{{
 *   <property>
 *   <name>mapreduce.outputcommitter.factory.scheme.s3a</name>
 *   <value>org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory</value>
 *   <description>
 *     The committer factory to use when writing data to S3A filesystems.
 *     If mapreduce.outputcommitter.factory.class is set, it will
 *     override this property.
 *   </description>
 * </property>
 * }}}
 *
 * == Binding a Spark Context to use the new committers for a store ==
 *
 * Spark uses the Hadoop committers in
 * [[org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand]]
 * by instantiating and then invoking an instance of
 * [[org.apache.spark.internal.io.HadoopMapReduceCommitProtocol]].
 * `InsertIntoHadoopFsRelationCommand` needs to be configured to use
 * [[org.apache.spark.internal.io.cloud.PathOutputCommitProtocol]] as
 * the commit protocol to use. This instantiates the committer through
 * the factory mechanism, and relays operations to it.
 *
 * When working with Parquet data, you need to explicitly switch
 * the Parquet committers to use the same mechanism
 *
 * In `spark-defaults.conf`, everything can be set up with the following settings:
 * {{{
 *   spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
 *   spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
 * }}}
 *
 * It can be done programmatically by calling [[cloud.bind()]] on the
 * spark configuration.
 */
package object cloud {

  /**
   * The classname to use when referring to the path output committer.
   */
  val PATH_COMMIT_PROTOCOL_CLASSNAME: String = classOf[PathOutputCommitProtocol].getName

  /**
   * The name of the parquet committer.
   */
  val PARQUET_COMMITTER_CLASSNAME: String = classOf[BindingParquetOutputCommitter].getName

  /**
   * Options for committer setup.
   * When applied to a spark configuration, this will set the
   * Dataframe output to use the factory mechanism for writing data for
   * all file formats.
   */
  val COMMITTER_BINDING_OPTIONS: Map[String, String] = Map(
    SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key -> PARQUET_COMMITTER_CLASSNAME,
    SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key -> PATH_COMMIT_PROTOCOL_CLASSNAME)

  /**
   * Set the options defined in [[cloud.COMMITTER_BINDING_OPTIONS]] on the
   * spark context.
   *
   * Warning: this is purely experimental.
   * @param sparkConf spark configuration to bind.
   */
  def bind(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(COMMITTER_BINDING_OPTIONS)
  }

}
