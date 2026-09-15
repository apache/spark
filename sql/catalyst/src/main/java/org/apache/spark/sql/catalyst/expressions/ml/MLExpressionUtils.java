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

package org.apache.spark.sql.catalyst.expressions.ml;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.ArrayData;

public class MLExpressionUtils {
  private static final byte SPARSE_VECTOR_TYPE = 0;
  private static final byte DENSE_VECTOR_TYPE = 1;

  private MLExpressionUtils() {}

  public static InternalRow scaleShift(
      InternalRow vector,
      ArrayData scale,
      ArrayData shift) {
    return scaleShift(vector, scale, shift, null, null);
  }

  public static InternalRow scaleShift(
      InternalRow vector,
      ArrayData scale,
      ArrayData shift,
      double[] cachedScale,
      double[] cachedShift) {
    boolean hasScale = scale != null || cachedScale != null;
    boolean hasShift = shift != null || cachedShift != null;
    if (!hasScale && !hasShift) {
      return vector;
    }

    byte vectorType = vector.getByte(0);
    ArrayData vectorValues = vector.getArray(3);
    int size;
    if (vectorType == SPARSE_VECTOR_TYPE) {
      size = vector.getInt(1);
    } else if (vectorType == DENSE_VECTOR_TYPE) {
      size = vectorValues.numElements();
    } else {
      throw new IllegalArgumentException("Unknown vector type " + vectorType + ".");
    }

    int scaleSize = !hasScale ? size :
      (cachedScale == null ? scale.numElements() : cachedScale.length);
    int shiftSize = !hasShift ? size :
      (cachedShift == null ? shift.numElements() : cachedShift.length);
    if (size != scaleSize || size != shiftSize) {
      throw new IllegalArgumentException(
        "requirement failed: VectorScaleShift was given inputs with non-matching sizes: " +
          "vector.size = " + size + ", scale.size = " + scaleSize +
          ", shift.size = " + shiftSize);
    }

    if (vectorType == SPARSE_VECTOR_TYPE && !hasShift) {
      ArrayData vectorIndices = vector.getArray(2);
      double[] resultValues = new double[vectorValues.numElements()];
      if (cachedScale == null) {
        for (int vectorIndex = 0; vectorIndex < resultValues.length; vectorIndex++) {
          int featureIndex = vectorIndices.getInt(vectorIndex);
          resultValues[vectorIndex] =
            vectorValues.getDouble(vectorIndex) * scale.getDouble(featureIndex);
        }
      } else {
        for (int vectorIndex = 0; vectorIndex < resultValues.length; vectorIndex++) {
          int featureIndex = vectorIndices.getInt(vectorIndex);
          resultValues[vectorIndex] =
            vectorValues.getDouble(vectorIndex) * cachedScale[featureIndex];
        }
      }
      return new GenericInternalRow(new Object[] {
        SPARSE_VECTOR_TYPE,
        size,
        vectorIndices,
        UnsafeArrayData.fromPrimitiveArray(resultValues)
      });
    }

    double[] resultValues = new double[size];
    if (vectorType == DENSE_VECTOR_TYPE) {
      if (!hasScale) {
        if (cachedShift == null) {
          for (int featureIndex = 0; featureIndex < size; featureIndex++) {
            resultValues[featureIndex] =
              vectorValues.getDouble(featureIndex) + shift.getDouble(featureIndex);
          }
        } else {
          for (int featureIndex = 0; featureIndex < size; featureIndex++) {
            resultValues[featureIndex] =
              vectorValues.getDouble(featureIndex) + cachedShift[featureIndex];
          }
        }
      } else if (!hasShift) {
        if (cachedScale == null) {
          for (int featureIndex = 0; featureIndex < size; featureIndex++) {
            resultValues[featureIndex] =
              vectorValues.getDouble(featureIndex) * scale.getDouble(featureIndex);
          }
        } else {
          for (int featureIndex = 0; featureIndex < size; featureIndex++) {
            resultValues[featureIndex] =
              vectorValues.getDouble(featureIndex) * cachedScale[featureIndex];
          }
        }
      } else if (cachedScale != null && cachedShift != null) {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = vectorValues.getDouble(featureIndex) *
            cachedScale[featureIndex] + cachedShift[featureIndex];
        }
      } else if (cachedScale != null) {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = vectorValues.getDouble(featureIndex) *
            cachedScale[featureIndex] + shift.getDouble(featureIndex);
        }
      } else if (cachedShift != null) {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = vectorValues.getDouble(featureIndex) *
            scale.getDouble(featureIndex) + cachedShift[featureIndex];
        }
      } else {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = vectorValues.getDouble(featureIndex) *
            scale.getDouble(featureIndex) + shift.getDouble(featureIndex);
        }
      }
    } else {
      if (cachedShift == null) {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = 0.0 + shift.getDouble(featureIndex);
        }
      } else {
        for (int featureIndex = 0; featureIndex < size; featureIndex++) {
          resultValues[featureIndex] = 0.0 + cachedShift[featureIndex];
        }
      }

      ArrayData vectorIndices = vector.getArray(2);
      if (!hasScale) {
        for (int vectorIndex = 0; vectorIndex < vectorValues.numElements(); vectorIndex++) {
          int featureIndex = vectorIndices.getInt(vectorIndex);
          resultValues[featureIndex] += vectorValues.getDouble(vectorIndex);
        }
      } else if (cachedScale == null) {
        for (int vectorIndex = 0; vectorIndex < vectorValues.numElements(); vectorIndex++) {
          int featureIndex = vectorIndices.getInt(vectorIndex);
          resultValues[featureIndex] +=
            vectorValues.getDouble(vectorIndex) * scale.getDouble(featureIndex);
        }
      } else {
        for (int vectorIndex = 0; vectorIndex < vectorValues.numElements(); vectorIndex++) {
          int featureIndex = vectorIndices.getInt(vectorIndex);
          resultValues[featureIndex] +=
            vectorValues.getDouble(vectorIndex) * cachedScale[featureIndex];
        }
      }
    }
    return new GenericInternalRow(new Object[] {
      DENSE_VECTOR_TYPE,
      null,
      null,
      UnsafeArrayData.fromPrimitiveArray(resultValues)
    });
  }
}
