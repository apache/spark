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

import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * :: DeveloperApi ::
 *
 * Trait for models and transformers which may be saved as files.
 * This should be inherited by the class which implements model instances.
 */
@DeveloperApi
@Since("1.3.0")
trait Saveable {

  /**
   * Save this model to the given path.
   *
   * This saves:
   *  - human-readable (JSON) model metadata to path/metadata/
   *  - Parquet formatted data to path/data/
   *
   * The model may be loaded using `Loader.load`.
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("1.3.0")
  def save(sc: SparkContext, path: String): Unit

  /** Current version of model save/load format. */
  protected def formatVersion: String

}

/**
 * :: DeveloperApi ::
 *
 * Trait for classes which can load models and transformers from files.
 * This should be inherited by an object paired with the model class.
 */
@DeveloperApi
@Since("1.3.0")
trait Loader[M <: Saveable] {

  /**
   * Load a model from the given path.
   *
   * The model should have been saved by `Saveable.save`.
   *
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  @Since("1.3.0")
  def load(sc: SparkContext, path: String): M

}

/**
 * Helper methods for loading models from files.
 */
private[mllib] object Loader {

  /** Returns URI for path/data using the Hadoop filesystem */
  def dataPath(path: String): String = new Path(path, "data").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  /**
   * Check the schema of loaded model data.
   *
   * This checks every field in the expected schema to make sure that a field with the same
   * name and DataType appears in the loaded schema.  Note that this does NOT check metadata
   * or containsNull.
   *
   * @param loadedSchema  Schema for model data loaded from file.
   * @tparam Data  Expected data type from which an expected schema can be derived.
   */
  def checkSchema[Data: TypeTag](loadedSchema: StructType): Unit = {
    // Check schema explicitly since erasure makes it hard to use match-case for checking.
    val expectedFields: Array[StructField] =
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].fields
    val loadedFields: Map[String, DataType] =
      loadedSchema.map(field => field.name -> field.dataType).toMap
    expectedFields.foreach { field =>
      assert(loadedFields.contains(field.name), s"Unable to parse model data." +
        s"  Expected field with name ${field.name} was missing in loaded schema:" +
        s" ${loadedFields.mkString(", ")}")
      assert(loadedFields(field.name).sameType(field.dataType),
        s"Unable to parse model data.  Expected field $field but found field" +
          s" with different type: ${loadedFields(field.name)}")
    }
  }

  /**
   * Load metadata from the given path.
   * @return (class name, version, metadata)
   */
  def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }
}
