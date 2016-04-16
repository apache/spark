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

package org.apache.spark.sql.types

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

/**
 * This object keeps the mappings between user classes and their User Defined Types (UDTs).
 * Previously we use the annotation `SQLUserDefinedType` to register UDTs for user classes.
 * However, by doing this, we add SparkSQL dependency on user classes. This object provides
 * alterntive approach to register UDTs for user classes.
 */
private[spark]
object UDTRegistration extends Serializable with Logging {

  /** The mapping between the Class between UserDefinedType and user classes. */
  private val udtMap: mutable.Map[String, Class[_]] =
    mutable.HashMap.empty[String, Class[_]]

  /**
   * Queries if a given user class is already registered or not.
   * @param userClassName the name of user class
   * @return boolean value indicates if the given user class is registered or not
   */
  def exists(userClassName: String): Boolean = udtMap.contains(userClassName)

  /**
   * Queries if a given user class is already registered or not.
   * @param userClass Class object of user class
   * @return boolean value indicates if the given user class is registered or not
   */
  def exists(userClass: Class[_]): Boolean = udtMap.contains(userClass.getName)

  /**
   * Registers an UserDefinedType to an user class. If the user class is already registered
   * with another UserDefinedType, an exception will be thrown.
   * @param userClass Class object of user class
   * @param udtClass the Class object of UserDefinedType
   */
  def register(userClass: Class[_], udtClass: Class[_]): Unit = {
    if (udtMap.contains(userClass.getName)) {
      logWarning(s"Cannot register UDT for ${userClass.getName}, which is already registered.")
    } else {
      if (classOf[UserDefinedType[_]].isAssignableFrom(udtClass)) {
        udtMap += ((userClass.getName, udtClass))
      } else {
        throw new SparkException(s"${udtClass.getName} is not an UserDefinedType.")
      }
    }
  }

  /**
   * Returns the Class of UserDefinedType for the name of a given user class.
   * @param userClass Class object of user class
   * @return Option value of the Class object of UserDefinedType
   */
  def getUDTFor(userClass: Class[_]): Option[Class[_]] =
    udtMap.get(userClass.getName)
}
