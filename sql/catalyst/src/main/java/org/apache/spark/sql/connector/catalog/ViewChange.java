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
package org.apache.spark.sql.connector.catalog;

import java.util.Objects;

import org.apache.spark.annotation.Evolving;

/**
 * ViewChange subclasses represent requested changes to a view. These are passed to
 * {@link ViewCatalog#alterView}.
 *
 * @since 5.0.0
 */
@Evolving
public interface ViewChange {

  /**
   * Create a ViewChange for setting a view property.
   *
   * @param property the property name
   * @param value the new property value
   * @return a ViewChange for setting the property
   */
  static ViewChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a ViewChange for removing a view property.
   * <p>
   * If the property does not exist, the change will succeed.
   *
   * @param property the property name
   * @return a ViewChange for removing the property
   */
  static ViewChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A ViewChange to set a view property. */
  final class SetProperty implements ViewChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public String property() { return property; }

    public String value() { return value; }

    @Override
    public String toString() { return "SET PROPERTY " + property + " = " + value; }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      SetProperty that = (SetProperty) other;
      return property.equals(that.property) && value.equals(that.value);
    }

    @Override
    public int hashCode() { return Objects.hash(property, value); }
  }

  /**
   * A ViewChange to remove a view property.
   * <p>
   * If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements ViewChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() { return property; }

    @Override
    public String toString() { return "REMOVE PROPERTY " + property; }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      RemoveProperty that = (RemoveProperty) other;
      return property.equals(that.property);
    }

    @Override
    public int hashCode() { return Objects.hash(property); }
  }
}
