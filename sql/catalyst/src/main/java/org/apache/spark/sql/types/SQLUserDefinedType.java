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

package org.apache.spark.sql.types;

import java.lang.annotation.*;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.annotation.InterfaceStability;

/**
 * ::DeveloperApi::
 * A user-defined type which can be automatically recognized by a SQLContext and registered.
 * WARNING: UDTs are currently only supported from Scala.
 */
// TODO: Should I used @Documented ?
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@InterfaceStability.Evolving
public @interface SQLUserDefinedType {

  /**
   * Returns an instance of the UserDefinedType which can serialize and deserialize the user
   * class to and from Catalyst built-in types.
   */
  Class<? extends UserDefinedType<?>> udt();
}
