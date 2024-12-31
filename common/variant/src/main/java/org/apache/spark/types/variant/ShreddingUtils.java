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

import java.math.BigDecimal;
import java.util.ArrayList;

import static org.apache.spark.types.variant.VariantUtil.*;

public class ShreddingUtils {
  // Interface to read from a shredded result. It essentially has the same interface and semantics
  // as Spark's `SpecializedGetters`, but we need a new interface to avoid the dependency.
  public interface ShreddedRow {
    boolean isNullAt(int ordinal);
    boolean getBoolean(int ordinal);
    byte getByte(int ordinal);
    short getShort(int ordinal);
    int getInt(int ordinal);
    long getLong(int ordinal);
    float getFloat(int ordinal);
    double getDouble(int ordinal);
    BigDecimal getDecimal(int ordinal, int precision, int scale);
    String getString(int ordinal);
    byte[] getBinary(int ordinal);
    ShreddedRow getStruct(int ordinal, int numFields);
    ShreddedRow getArray(int ordinal);
    int numElements();
  }

  // This `rebuild` function should only be called on the top-level schema, and that other private
  // implementation will be called on any recursively shredded sub-schema.
  public static Variant rebuild(ShreddedRow row, VariantSchema schema) {
    if (schema.topLevelMetadataIdx < 0 || row.isNullAt(schema.topLevelMetadataIdx)) {
      throw malformedVariant();
    }
    byte[] metadata = row.getBinary(schema.topLevelMetadataIdx);
    if (schema.isUnshredded()) {
      // `rebuild` is unnecessary for unshredded variant.
      if (row.isNullAt(schema.variantIdx)) {
        throw malformedVariant();
      }
      return new Variant(row.getBinary(schema.variantIdx), metadata);
    }
    VariantBuilder builder = new VariantBuilder(false);
    rebuild(row, metadata, schema, builder);
    return builder.result();
  }

  // Rebuild a variant value from the shredded data according to the reconstruction algorithm in
  // https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
  // Append the result to `builder`.
  public static void rebuild(ShreddedRow row, byte[] metadata, VariantSchema schema,
                             VariantBuilder builder) {
    int typedIdx = schema.typedIdx;
    int variantIdx = schema.variantIdx;
    if (typedIdx >= 0 && !row.isNullAt(typedIdx)) {
      if (schema.scalarSchema != null) {
        VariantSchema.ScalarType scalar = schema.scalarSchema;
        if (scalar instanceof VariantSchema.StringType) {
          builder.appendString(row.getString(typedIdx));
        } else if (scalar instanceof VariantSchema.IntegralType) {
          VariantSchema.IntegralType it = (VariantSchema.IntegralType) scalar;
          long value = 0;
          switch (it.size) {
            case BYTE:
              value = row.getByte(typedIdx);
              break;
            case SHORT:
              value = row.getShort(typedIdx);
              break;
            case INT:
              value = row.getInt(typedIdx);
              break;
            case LONG:
              value = row.getLong(typedIdx);
              break;
          }
          builder.appendLong(value);
        } else if (scalar instanceof VariantSchema.FloatType) {
          builder.appendFloat(row.getFloat(typedIdx));
        } else if (scalar instanceof VariantSchema.DoubleType) {
          builder.appendDouble(row.getDouble(typedIdx));
        } else if (scalar instanceof VariantSchema.BooleanType) {
          builder.appendBoolean(row.getBoolean(typedIdx));
        } else if (scalar instanceof VariantSchema.BinaryType) {
          builder.appendBinary(row.getBinary(typedIdx));
        } else if (scalar instanceof VariantSchema.DecimalType) {
          VariantSchema.DecimalType dt = (VariantSchema.DecimalType) scalar;
          builder.appendDecimal(row.getDecimal(typedIdx, dt.precision, dt.scale));
        } else if (scalar instanceof VariantSchema.DateType) {
          builder.appendDate(row.getInt(typedIdx));
        } else if (scalar instanceof VariantSchema.TimestampType) {
          builder.appendTimestamp(row.getLong(typedIdx));
        } else {
          assert scalar instanceof VariantSchema.TimestampNTZType;
          builder.appendTimestampNtz(row.getLong(typedIdx));
        }
      } else if (schema.arraySchema != null) {
        VariantSchema elementSchema = schema.arraySchema;
        ShreddedRow array = row.getArray(typedIdx);
        int start = builder.getWritePos();
        ArrayList<Integer> offsets = new ArrayList<>(array.numElements());
        for (int i = 0; i < array.numElements(); i++) {
          offsets.add(builder.getWritePos() - start);
          rebuild(array.getStruct(i, elementSchema.numFields), metadata, elementSchema, builder);
        }
        builder.finishWritingArray(start, offsets);
      } else {
        ShreddedRow object = row.getStruct(typedIdx, schema.objectSchema.length);
        ArrayList<VariantBuilder.FieldEntry> fields = new ArrayList<>();
        int start = builder.getWritePos();
        for (int fieldIdx = 0; fieldIdx < schema.objectSchema.length; ++fieldIdx) {
          // Shredded field must not be null.
          if (object.isNullAt(fieldIdx)) {
            throw malformedVariant();
          }
          String fieldName = schema.objectSchema[fieldIdx].fieldName;
          VariantSchema fieldSchema = schema.objectSchema[fieldIdx].schema;
          ShreddedRow fieldValue = object.getStruct(fieldIdx, fieldSchema.numFields);
          // If the field doesn't have non-null `typed_value` or `value`, it is missing.
          if ((fieldSchema.typedIdx >= 0 && !fieldValue.isNullAt(fieldSchema.typedIdx)) ||
              (fieldSchema.variantIdx >= 0 && !fieldValue.isNullAt(fieldSchema.variantIdx))) {
            int id = builder.addKey(fieldName);
            fields.add(new VariantBuilder.FieldEntry(fieldName, id, builder.getWritePos() - start));
            rebuild(fieldValue, metadata, fieldSchema, builder);
          }
        }
        if (variantIdx >= 0 && !row.isNullAt(variantIdx)) {
          // Add the leftover fields in the variant binary.
          Variant v = new Variant(row.getBinary(variantIdx), metadata);
          if (v.getType() != VariantUtil.Type.OBJECT) throw malformedVariant();
          for (int i = 0; i < v.objectSize(); ++i) {
            Variant.ObjectField field = v.getFieldAtIndex(i);
            // `value` must not contain any shredded field.
            if (schema.objectSchemaMap.containsKey(field.key)) {
              throw malformedVariant();
            }
            int id = builder.addKey(field.key);
            fields.add(new VariantBuilder.FieldEntry(field.key, id, builder.getWritePos() - start));
            builder.appendVariant(field.value);
          }
        }
        builder.finishWritingObject(start, fields);
      }
    } else if (variantIdx >= 0 && !row.isNullAt(variantIdx)) {
      // `typed_value` doesn't exist or is null. Read from `value`.
      builder.appendVariant(new Variant(row.getBinary(variantIdx), metadata));
    } else {
      // This means the variant is missing in a context where it must present, so the input data is
      // invalid.
      throw malformedVariant();
    }
  }
}
