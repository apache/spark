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

package org.apache.spark.sql.catalyst.annotation;

import java.lang.annotation.*;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.types.UserDefinedType;

/**
 * ::DeveloperApi::
 * A user-defined type which can be automatically recognized by a SQLContext and registered.
 *
 * WARNING: This annotation will only work if both Java and Scala reflection return the same class
 *          names (after erasure) for the UDT.  This will NOT be the case when, e.g., the UDT class
 *          is enclosed in an object (a singleton).
 *
 * WARNING: UDTs are currently only supported from Scala.
 */
// TODO: Should I used @Documented ?
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SQLUserDefinedType {

  /**
   * Returns an instance of the UserDefinedType which can serialize and deserialize the user
   * class to and from Catalyst built-in types.
   */
  Class<? extends UserDefinedType<?> > udt();
}
