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

package org.apache.spark.sql.connector.catalog.procedures;

import javax.annotation.Nullable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.DefaultValue;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.internal.connector.ProcedureParameterImpl;
import org.apache.spark.sql.types.DataType;

import static org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter.Mode.IN;

/**
 * A {@link Procedure procedure} parameter.
 *
 * @since 4.0.0
 */
@Evolving
public interface ProcedureParameter {
  /**
   * A field metadata key that indicates whether an argument is passed by name.
   */
  String BY_NAME_METADATA_KEY = "BY_NAME";

  /**
   * Creates a builder for an IN procedure parameter.
   *
   * @param name the name of the parameter
   * @param dataType the type of the parameter
   * @return the constructed stored procedure parameter
   */
  static Builder in(String name, DataType dataType) {
    return new Builder(IN, name, dataType);
  }

  /**
   * Returns the mode of this parameter.
   */
  Mode mode();

  /**
   * Returns the name of this parameter.
   */
  String name();

  /**
   * Returns the data type of this parameter.
   */
  DataType dataType();

  /**
   * Returns the SQL string (Spark SQL dialect) of the default value expression of this parameter or
   * null if not provided.
   */
  @Nullable
  DefaultValue defaultValue();

  /**
   * Returns the comment of this parameter or null if not provided.
   */
  @Nullable
  String comment();

  /**
   * An enum representing procedure parameter modes.
   */
  enum Mode {
    IN,
    INOUT,
    OUT
  }

  class Builder {
    private final Mode mode;
    private final String name;
    private final DataType dataType;
    private DefaultValue defaultValue;
    private String comment;

    private Builder(Mode mode, String name, DataType dataType) {
      this.mode = mode;
      this.name = name;
      this.dataType = dataType;
    }

    /**
     * Sets the default value of the parameter using SQL.
     */
    public Builder defaultValue(String sql) {
      this.defaultValue = new DefaultValue(sql);
      return this;
    }

    /**
     * Sets the default value of the parameter using an expression.
     */
    public Builder defaultValue(Expression expression) {
      this.defaultValue = new DefaultValue(expression);
      return this;
    }

    /**
     * Sets the default value of the parameter.
     */
    public Builder defaultValue(DefaultValue defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    /**
     * Sets the comment of the parameter.
     */
    public Builder comment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Builds the stored procedure parameter.
     */
    public ProcedureParameter build() {
      return new ProcedureParameterImpl(mode, name, dataType, defaultValue, comment);
    }
  }
}
