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

package org.apache.spark.sql.api.java;

import java.io.Serializable;

import org.apache.spark.annotation.DeveloperApi;

/**
 * ::DeveloperApi::
 * The data type representing User-Defined Types (UDTs).
 * UDTs may use any other DataType for an underlying representation.
 */
@DeveloperApi
public abstract class UserDefinedType<UserType> extends DataType implements Serializable {

  protected UserDefinedType() { }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UserDefinedType<UserType> that = (UserDefinedType<UserType>) o;
    return this.sqlType().equals(that.sqlType());
  }

  /** Underlying storage type for this UDT */
  public abstract DataType sqlType();

  /** Convert the user type to a SQL datum */
  public abstract Object serialize(Object obj);

  /** Convert a SQL datum to the user type */
  public abstract UserType deserialize(Object datum);

  /** Class object for the UserType */
  public abstract Class<UserType> userClass();
}
