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
import java.math.RoundingMode;
import java.util.ArrayList;

/**
 * Class to implement shredding a Variant value.
 */
public class VariantShreddingWriter {

  // Interface to build up a shredded result. Callers should implement a ShreddedResultBuilder to
  // create an empty result with a given schema. The castShredded method will call one or more of
  // the add* methods to populate it.
  public interface ShreddedResult {
    // Create an array. The elements are the result of shredding each element.
    void addArray(ShreddedResult[] array);
    // Create an object. The values are the result of shredding each field, order by the index in
    // objectSchema. Missing fields are populated with an empty result.
    void addObject(ShreddedResult[] values);
    void addVariantValue(byte[] result);
    // Add a scalar to typed_value. The type of Object depends on the scalarSchema in the shredding
    // schema.
    void addScalar(Object result);
    void addMetadata(byte[] result);
  }

  public interface ShreddedResultBuilder {
    ShreddedResult createEmpty(VariantSchema schema);

    // If true, we will shred decimals to a different scale or to integers, as long as they are
    // numerically equivalent. Similarly, integers will be allowed to shred to decimals.
    boolean allowNumericScaleChanges();
  }

  /**
   * Converts an input variant into shredded components. Returns the shredded result, as well
   * as the original Variant with shredded fields removed.
   * `dataType` must be a valid shredding schema, as described in
   * https://github.com/apache/parquet-format/blob/master/VariantShredding.md.
   */
  public static ShreddedResult castShredded(
          Variant v,
          VariantSchema schema,
          ShreddedResultBuilder builder) {
    VariantUtil.Type variantType = v.getType();
    ShreddedResult result = builder.createEmpty(schema);

    if (schema.topLevelMetadataIdx >= 0) {
      result.addMetadata(v.getMetadata());
    }

    if (schema.arraySchema != null && variantType == VariantUtil.Type.ARRAY) {
      // The array element is always a struct containing untyped and typed fields.
      VariantSchema elementSchema = schema.arraySchema;
      int size = v.arraySize();
      ShreddedResult[] array = new ShreddedResult[size];
      for (int i = 0; i < size; ++i) {
        ShreddedResult shreddedArray = castShredded(v.getElementAtIndex(i), elementSchema, builder);
        array[i] = shreddedArray;
      }
      result.addArray(array);
    } else if (schema.objectSchema != null && variantType == VariantUtil.Type.OBJECT) {
      VariantSchema.ObjectField[] objectSchema = schema.objectSchema;
      ShreddedResult[] shreddedValues = new ShreddedResult[objectSchema.length];

      // Create a variantBuilder for any field that exist in `v`, but not in the shredding schema.
      VariantBuilder variantBuilder = new VariantBuilder(false);
      ArrayList<VariantBuilder.FieldEntry> fieldEntries = new ArrayList<>();
      // Keep track of which schema fields we actually found in the Variant value.
      int numFieldsMatched = 0;
      int start = variantBuilder.getWritePos();
      for (int i = 0; i < v.objectSize(); ++i) {
        Variant.ObjectField field = v.getFieldAtIndex(i);
        Integer fieldIdx = schema.objectSchemaMap.get(field.key);
        if (fieldIdx != null) {
          // The field exists in the shredding schema. Recursively shred, and write the result.
          ShreddedResult shreddedField = castShredded(
              field.value, objectSchema[fieldIdx].schema, builder);
          shreddedValues[fieldIdx] = shreddedField;
          numFieldsMatched++;
        } else {
          // The field is not shredded. Put it in the untyped_value column.
          int id = v.getDictionaryIdAtIndex(i);
          fieldEntries.add(new VariantBuilder.FieldEntry(
              field.key, id, variantBuilder.getWritePos() - start));
          // shallowAppendVariant is needed for correctness, since we're relying on the metadata IDs
          // being unchanged.
          variantBuilder.shallowAppendVariant(field.value);
        }
      }
      if (numFieldsMatched < objectSchema.length) {
        // Set missing fields to non-null with all fields set to null.
        for (int i = 0; i < objectSchema.length; ++i) {
          if (shreddedValues[i] == null) {
            VariantSchema.ObjectField fieldSchema = objectSchema[i];
            ShreddedResult emptyChild = builder.createEmpty(fieldSchema.schema);
            shreddedValues[i] = emptyChild;
            numFieldsMatched += 1;
          }
        }
      }
      if (numFieldsMatched != objectSchema.length) {
        // Since we just filled in all the null entries, this can only happen if we tried to write
        // to the same field twice; i.e. the Variant contained duplicate fields, which is invalid.
        throw VariantUtil.malformedVariant();
      }
      result.addObject(shreddedValues);
      if (variantBuilder.getWritePos() != start) {
        // We added something to the untyped value.
        variantBuilder.finishWritingObject(start, fieldEntries);
        result.addVariantValue(variantBuilder.valueWithoutMetadata());
      }
    } else if (schema.scalarSchema != null) {
      VariantSchema.ScalarType scalarType = schema.scalarSchema;
      Object typedValue = tryTypedShred(v, variantType, scalarType, builder);
      if (typedValue != null) {
        // Store the typed value.
        result.addScalar(typedValue);
      } else {
        result.addVariantValue(v.getValue());
      }
    } else {
      // Store in untyped.
      result.addVariantValue(v.getValue());
    }
    return result;
  }

  /**
   * Tries to cast a Variant into a typed value. If the cast fails, returns null.
   *
   * @param v
   * @param variantType The Variant Type of v
   * @param targetType The target type
   * @return The scalar value, or null if the cast is not valid.
   */
  private static Object tryTypedShred(
          Variant v,
          VariantUtil.Type variantType,
          VariantSchema.ScalarType targetType,
          ShreddedResultBuilder builder) {
    switch (variantType) {
      case LONG:
        if (targetType instanceof VariantSchema.IntegralType integralType) {
          // Check that the target type can hold the actual value.
          VariantSchema.IntegralSize size = integralType.size;
          long value = v.getLong();
          switch (size) {
            case BYTE:
              if (value == (byte) value) {
                  return (byte) value;
              }
              break;
            case SHORT:
              if (value == (short) value) {
                  return (short) value;
              }
              break;
            case INT:
              if (value == (int) value) {
                  return (int) value;
              }
              break;
            case LONG:
              return value;
          }
        } else if (targetType instanceof VariantSchema.DecimalType decimalType &&
                   builder.allowNumericScaleChanges()) {
          // If the integer can fit in the given decimal precision, allow it.
          long value = v.getLong();
          // Set to the requested scale, and check if the precision is large enough.
          BigDecimal decimalValue = BigDecimal.valueOf(value);
          BigDecimal scaledValue = decimalValue.setScale(decimalType.scale);
          // The initial value should have scale 0, so rescaling shouldn't lose information.
          assert(decimalValue.compareTo(scaledValue) == 0);
          if (scaledValue.precision() <= decimalType.precision) {
            return scaledValue;
          }
        }
        break;
      case DECIMAL:
        if (targetType instanceof VariantSchema.DecimalType decimalType) {
          // Use getDecimalWithOriginalScale so that we retain scale information if
          // allowNumericScaleChanges() is false.
          BigDecimal value = VariantUtil.getDecimalWithOriginalScale(v.value, v.pos);
          if (value.precision() <= decimalType.precision &&
              value.scale() == decimalType.scale) {
            return value;
          }
          if (builder.allowNumericScaleChanges()) {
            // Convert to the target scale, and see if it fits. Rounding mode doesn't matter,
            // since we'll reject it if it turned out to require rounding.
            BigDecimal scaledValue = value.setScale(decimalType.scale, RoundingMode.FLOOR);
            if (scaledValue.compareTo(value) == 0 &&
                    scaledValue.precision() <= decimalType.precision) {
              return scaledValue;
            }
          }
        } else if (targetType instanceof VariantSchema.IntegralType integralType &&
          builder.allowNumericScaleChanges()) {
          // Check if the decimal happens to be an integer.
          BigDecimal value = v.getDecimal();
          VariantSchema.IntegralSize size = integralType.size;
          // Try to cast to the appropriate type, and check if any information is lost.
          switch (size) {
            case BYTE:
              if (value.compareTo(BigDecimal.valueOf(value.byteValue())) == 0) {
                return value.byteValue();
              }
              break;
            case SHORT:
              if (value.compareTo(BigDecimal.valueOf(value.shortValue())) == 0) {
                return value.shortValue();
              }
              break;
            case INT:
              if (value.compareTo(BigDecimal.valueOf(value.intValue())) == 0) {
                return value.intValue();
              }
              break;
            case LONG:
              if (value.compareTo(BigDecimal.valueOf(value.longValue())) == 0) {
                return value.longValue();
              }
          }
        }
        break;
      case BOOLEAN:
        if (targetType instanceof VariantSchema.BooleanType) {
          return v.getBoolean();
        }
        break;
      case STRING:
        if (targetType instanceof VariantSchema.StringType) {
          return v.getString();
        }
        break;
      case DOUBLE:
        if (targetType instanceof VariantSchema.DoubleType) {
          return v.getDouble();
        }
        break;
      case DATE:
        if (targetType instanceof VariantSchema.DateType) {
          return (int) v.getLong();
        }
        break;
      case TIMESTAMP:
        if (targetType instanceof VariantSchema.TimestampType) {
          return v.getLong();
        }
        break;
      case TIMESTAMP_NTZ:
        if (targetType instanceof VariantSchema.TimestampNTZType) {
          return v.getLong();
        }
        break;
      case FLOAT:
        if (targetType instanceof VariantSchema.FloatType) {
          return v.getFloat();
        }
        break;
      case BINARY:
        if (targetType instanceof VariantSchema.BinaryType) {
          return v.getBinary();
        }
        break;
      case UUID:
        if (targetType instanceof VariantSchema.UuidType) {
          return v.getUuid();
        }
        break;
    }
    // The stored type does not match the requested shredding type. Return null, and the caller
    // will store the result in untyped_value.
    return null;
  }

  // Add the result to the shredding result.
  private static void addVariantValueVariant(Variant variantResult,
      VariantSchema schema, ShreddedResult result) {
    result.addVariantValue(variantResult.getValue());
  }

}
