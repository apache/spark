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

package org.apache.spark.sql.pipelines.utils

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.AnalysisException

/**
 * Collection of helper methods to simplify working with exceptions in tests.
 */
trait SparkErrorTestMixin {
  this: SparkFunSuite =>

  /**
   * Asserts that the given exception is a [[AnalysisException]] with the specific error class.
   */
  def assertAnalysisException(ex: Throwable, errorClass: String): Unit = {
    ex match {
      case t: AnalysisException =>
        assert(
          t.getCondition == errorClass,
          s"Expected analysis exception with error class $errorClass, but got ${t.getCondition}"
        )
      case _ => fail(s"Expected analysis exception but got ${ex.getClass}")
    }
  }

  /**
   * Asserts that the given exception is a [[AnalysisException]]h the specific error class
   * and metadata.
   */
  def assertAnalysisException(
      ex: Throwable,
      errorClass: String,
      metadata: Map[String, String]
  ): Unit = {
    ex match {
      case t: AnalysisException =>
        assert(
          t.getCondition == errorClass,
          s"Expected analysis exception with error class $errorClass, but got ${t.getCondition}"
        )
        assert(
          t.getMessageParameters.asScala.toMap == metadata,
          s"Expected analysis exception with metadata $metadata, but got " +
          s"${t.getMessageParameters}"
        )
      case _ => fail(s"Expected analysis exception but got ${ex.getClass}")
    }
  }

  /**
   * Asserts that the given exception is a [[SparkException]] with the specific error class.
   */
  def assertSparkException(ex: Throwable, errorClass: String): Unit = {
    ex match {
      case t: SparkException =>
        assert(
          t.getCondition == errorClass,
          s"Expected spark exception with error class $errorClass, but got ${t.getCondition}"
        )
      case _ => fail(s"Expected spark exception but got ${ex.getClass}")
    }
  }

  /**
   * Asserts that the given exception is a [[SparkException]] with the specific error class
   * and metadata.
   */
  def assertSparkException(
      ex: Throwable,
      errorClass: String,
      metadata: Map[String, String]
  ): Unit = {
    ex match {
      case t: SparkException =>
        assert(
          t.getCondition == errorClass,
          s"Expected spark exception with error class $errorClass, but got ${t.getCondition}"
        )
        assert(
          t.getMessageParameters.asScala.toMap == metadata,
          s"Expected spark exception with metadata $metadata, but got ${t.getMessageParameters}"
        )
      case _ => fail(s"Expected spark exception but got ${ex.getClass}")
    }
  }
}
