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

package org.apache.spark.types.variant;

import java.util.Map;

/**
 * Defines a valid shredding schema, as described in
 * https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
 * A shredding schema contains a value and optional typed_value field.
 * If a typed_value is an array or struct, it recursively contain its own shredding schema for
 * elements and fields, respectively.
 * The schema also contains a metadata field at the top level, but not in recursively shredded
 * fields.
 */
public class VariantSchema {

  // Represents one field of an object in the shredding schema.
  public static final class ObjectField {
    // The index of the field in the write schema. E.g. in Spark, it is the position within the
    // corresponding InternalRow. Indices for an object schema must be zero-indexed and dense; i.e.
    // they must cover the range [0, numFields - 1] with no gaps.
    public final int idx;
    public final VariantSchema schema;

    public ObjectField(int idx, VariantSchema schema) {
      this.idx = idx;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return "ObjectField{" +
          "idx=" + idx +
          ", schema=" + schema +
          '}';
    }
  }

  public static abstract class ScalarType {
  }

  public static final class StringType extends ScalarType {
  }

  public enum IntegralSize {
    BYTE, SHORT, INT, LONG
  }

  public static final class IntegralType extends ScalarType {
    public final IntegralSize size;

    public IntegralType(IntegralSize size) {
      this.size = size;
    }
  }

  public static final class FloatType extends ScalarType {
  }

  public static final class DoubleType extends ScalarType {
  }

  public static final class BooleanType extends ScalarType {
  }

  public static final class BinaryType extends ScalarType {
  }

  public static final class DecimalType extends ScalarType {
    public final int precision;
    public final int scale;

    public DecimalType(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }
  }

  public static final class DateType extends ScalarType {
  }

  public static final class TimestampType extends ScalarType {
  }

  public static final class TimestampNTZType extends ScalarType {
  }

  public final int typedIdx;
  public final int variantIdx;
  public final int topLevelMetadataIdx;
  public final int numFields;

  public final ScalarType scalarSchema;
  public final Map<String, ObjectField> objectSchema;
  public final VariantSchema arraySchema;

  public VariantSchema(int typedIdx, int variantIdx, int topLevelMetadataIdx, int numFields,
                       ScalarType scalarSchema, Map<String, ObjectField> objectSchema,
                       VariantSchema arraySchema) {
    this.typedIdx = typedIdx;
    this.numFields = numFields;
    this.variantIdx = variantIdx;
    this.topLevelMetadataIdx = topLevelMetadataIdx;
    this.scalarSchema = scalarSchema;
    this.objectSchema = objectSchema;
    this.arraySchema = arraySchema;
  }

  @Override
  public String toString() {
    return "VariantSchema{" +
        "typedIdx=" + typedIdx +
        ", variantIdx=" + variantIdx +
        ", topLevelMetadataIdx=" + topLevelMetadataIdx +
        ", numFields=" + numFields +
        ", scalarSchema=" + scalarSchema +
        ", objectSchema=" + objectSchema +
        ", arraySchema=" + arraySchema +
        '}';
  }
}
