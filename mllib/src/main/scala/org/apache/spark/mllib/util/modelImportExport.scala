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

package org.apache.spark.mllib.util

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * Trait for models and transformers which may be saved as files.
 * This should be inherited by the class which implements model instances.
 */
@DeveloperApi
trait Exportable {

  /**
   * Save this model to the given path.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using [[Importable.load]].
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              This directory and any intermediate directory will be created if needed.
   */
  def save(sc: SparkContext, path: String): Unit

}

object Exportable {

  /** Current version of model import/export format. */
  val latestVersion: String = "1.0"

}

/**
 * :: DeveloperApi ::
 *
 * Trait for models and transformers which may be loaded from files.
 * This should be inherited by an object paired with the model class.
 */
@DeveloperApi
trait Importable[Model <: Exportable] {

  /**
   * Load a model from the given path.
   *
   * The model should have been saved by [[Exportable.save]].
   *
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  def load(sc: SparkContext, path: String): Model

}
