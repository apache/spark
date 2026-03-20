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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A utility class for vector similarity/distance function implementations.
 */
public class VectorFunctionImplUtils {

  /**
   * Computes the cosine similarity between two float vectors.
   * Returns NULL if either vector contains NULL elements, has zero magnitude, or is empty.
   * Throws an exception if vectors have different dimensions.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Float vectorCosineSimilarity(ArrayData left, ArrayData right, UTF8String funcName) {
    int leftLen = left.numElements();
    int rightLen = right.numElements();

    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(
          funcName.toString(), leftLen, rightLen);
    }

    if (leftLen == 0) {
      return null;
    }

    float dotProduct = 0.0f;
    float norm1Sq = 0.0f;
    float norm2Sq = 0.0f;

    int i = 0;
    int simdLimit = (leftLen / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (left.isNullAt(i) || left.isNullAt(i + 1) ||
          left.isNullAt(i + 2) || left.isNullAt(i + 3) ||
          left.isNullAt(i + 4) || left.isNullAt(i + 5) ||
          left.isNullAt(i + 6) || left.isNullAt(i + 7) ||
          right.isNullAt(i) || right.isNullAt(i + 1) ||
          right.isNullAt(i + 2) || right.isNullAt(i + 3) ||
          right.isNullAt(i + 4) || right.isNullAt(i + 5) ||
          right.isNullAt(i + 6) || right.isNullAt(i + 7)) {
        return null;
      }

      float a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      float a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      float a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      float a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      float b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      float b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      float b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      float b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

      dotProduct += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3 +
                    a4 * b4 + a5 * b5 + a6 * b6 + a7 * b7;
      norm1Sq += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3 +
                 a4 * a4 + a5 * a5 + a6 * a6 + a7 * a7;
      norm2Sq += b0 * b0 + b1 * b1 + b2 * b2 + b3 * b3 +
                 b4 * b4 + b5 * b5 + b6 * b6 + b7 * b7;
      i += 8;
    }

    // Handle remaining elements
    while (i < leftLen) {
      if (left.isNullAt(i) || right.isNullAt(i)) {
        return null;
      }
      float a = left.getFloat(i);
      float b = right.getFloat(i);
      dotProduct += a * b;
      norm1Sq += a * a;
      norm2Sq += b * b;
      i++;
    }

    float normProduct = (float) Math.sqrt(norm1Sq * norm2Sq);
    if (normProduct < Float.MIN_NORMAL) {
      return null;
    }
    return dotProduct / normProduct;
  }

  /**
   * Computes the inner product (dot product) between two float vectors.
   * Returns NULL if either vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws an exception if vectors have different dimensions.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Float vectorInnerProduct(ArrayData left, ArrayData right, UTF8String funcName) {
    int leftLen = left.numElements();
    int rightLen = right.numElements();

    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(
          funcName.toString(), leftLen, rightLen);
    }

    if (leftLen == 0) {
      return 0.0f;
    }

    float dotProduct = 0.0f;

    int i = 0;
    int simdLimit = (leftLen / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (left.isNullAt(i) || left.isNullAt(i + 1) ||
          left.isNullAt(i + 2) || left.isNullAt(i + 3) ||
          left.isNullAt(i + 4) || left.isNullAt(i + 5) ||
          left.isNullAt(i + 6) || left.isNullAt(i + 7) ||
          right.isNullAt(i) || right.isNullAt(i + 1) ||
          right.isNullAt(i + 2) || right.isNullAt(i + 3) ||
          right.isNullAt(i + 4) || right.isNullAt(i + 5) ||
          right.isNullAt(i + 6) || right.isNullAt(i + 7)) {
        return null;
      }

      float a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      float a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      float a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      float a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      float b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      float b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      float b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      float b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

      dotProduct += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3 +
                    a4 * b4 + a5 * b5 + a6 * b6 + a7 * b7;
      i += 8;
    }

    // Handle remaining elements
    while (i < leftLen) {
      if (left.isNullAt(i) || right.isNullAt(i)) {
        return null;
      }
      float a = left.getFloat(i);
      float b = right.getFloat(i);
      dotProduct += a * b;
      i++;
    }

    return dotProduct;
  }

  /**
   * Computes the Euclidean (L2) distance between two float vectors.
   * Returns NULL if either vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws an exception if vectors have different dimensions.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Float vectorL2Distance(ArrayData left, ArrayData right, UTF8String funcName) {
    int leftLen = left.numElements();
    int rightLen = right.numElements();

    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(
          funcName.toString(), leftLen, rightLen);
    }

    if (leftLen == 0) {
      return 0.0f;
    }

    float sumSq = 0.0f;

    int i = 0;
    int simdLimit = (leftLen / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (left.isNullAt(i) || left.isNullAt(i + 1) ||
          left.isNullAt(i + 2) || left.isNullAt(i + 3) ||
          left.isNullAt(i + 4) || left.isNullAt(i + 5) ||
          left.isNullAt(i + 6) || left.isNullAt(i + 7) ||
          right.isNullAt(i) || right.isNullAt(i + 1) ||
          right.isNullAt(i + 2) || right.isNullAt(i + 3) ||
          right.isNullAt(i + 4) || right.isNullAt(i + 5) ||
          right.isNullAt(i + 6) || right.isNullAt(i + 7)) {
        return null;
      }

      float a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      float a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      float a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      float a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      float b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      float b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      float b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      float b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

      float d0 = a0 - b0, d1 = a1 - b1, d2 = a2 - b2, d3 = a3 - b3;
      float d4 = a4 - b4, d5 = a5 - b5, d6 = a6 - b6, d7 = a7 - b7;

      sumSq += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3 +
               d4 * d4 + d5 * d5 + d6 * d6 + d7 * d7;
      i += 8;
    }

    // Handle remaining elements
    while (i < leftLen) {
      if (left.isNullAt(i) || right.isNullAt(i)) {
        return null;
      }
      float a = left.getFloat(i);
      float b = right.getFloat(i);
      float diff = a - b;
      sumSq += diff * diff;
      i++;
    }

    return (float) Math.sqrt(sumSq);
  }

  /**
   * Computes the L1 norm (Manhattan norm) of a float vector.
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Float vectorL1Norm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0f;
    }

    float sum = 0.0f;

    int i = 0;
    int simdLimit = (len / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (vec.isNullAt(i) || vec.isNullAt(i + 1) ||
          vec.isNullAt(i + 2) || vec.isNullAt(i + 3) ||
          vec.isNullAt(i + 4) || vec.isNullAt(i + 5) ||
          vec.isNullAt(i + 6) || vec.isNullAt(i + 7)) {
        return null;
      }

      float a0 = vec.getFloat(i), a1 = vec.getFloat(i + 1);
      float a2 = vec.getFloat(i + 2), a3 = vec.getFloat(i + 3);
      float a4 = vec.getFloat(i + 4), a5 = vec.getFloat(i + 5);
      float a6 = vec.getFloat(i + 6), a7 = vec.getFloat(i + 7);

      sum += Math.abs(a0) + Math.abs(a1) + Math.abs(a2) + Math.abs(a3) +
             Math.abs(a4) + Math.abs(a5) + Math.abs(a6) + Math.abs(a7);
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      float a = vec.getFloat(i);
      sum += Math.abs(a);
      i++;
    }

    return sum;
  }

  /**
   * Computes the L2 norm (Euclidean norm) of a float vector.
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Float vectorL2Norm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0f;
    }

    float sumSq = 0.0f;

    int i = 0;
    int simdLimit = (len / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (vec.isNullAt(i) || vec.isNullAt(i + 1) ||
          vec.isNullAt(i + 2) || vec.isNullAt(i + 3) ||
          vec.isNullAt(i + 4) || vec.isNullAt(i + 5) ||
          vec.isNullAt(i + 6) || vec.isNullAt(i + 7)) {
        return null;
      }

      float a0 = vec.getFloat(i), a1 = vec.getFloat(i + 1);
      float a2 = vec.getFloat(i + 2), a3 = vec.getFloat(i + 3);
      float a4 = vec.getFloat(i + 4), a5 = vec.getFloat(i + 5);
      float a6 = vec.getFloat(i + 6), a7 = vec.getFloat(i + 7);

      sumSq += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3 +
               a4 * a4 + a5 * a5 + a6 * a6 + a7 * a7;
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      float a = vec.getFloat(i);
      sumSq += a * a;
      i++;
    }

    return (float) Math.sqrt(sumSq);
  }

  /**
   * Computes the infinity norm (maximum absolute value) of a float vector.
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   */
  public static Float vectorInfNorm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0f;
    }

    float maxAbs = 0.0f;
    for (int i = 0; i < len; i++) {
      if (vec.isNullAt(i)) {
        return null;
      }
      float absVal = Math.abs(vec.getFloat(i));
      if (absVal > maxAbs) {
        maxAbs = absVal;
      }
    }

    return maxAbs;
  }

  /**
   * Normalizes a float vector by dividing each element by the given norm.
   * Returns NULL if the vector contains NULL elements or if the norm is zero.
   * Returns an empty array for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static ArrayData vectorNormalizeWithNorm(ArrayData vec, float norm) {
    int len = vec.numElements();

    if (len == 0) {
      return vec;
    }

    if (norm < Float.MIN_NORMAL) {
      return null;
    }

    float[] result = new float[len];

    int i = 0;
    int simdLimit = (len / 8) * 8;

    // Manual unroll loop - process 8 floats at a time for speculative SIMD optimization
    while (i < simdLimit) {
      // Check for nulls in batch
      if (vec.isNullAt(i) || vec.isNullAt(i + 1) ||
          vec.isNullAt(i + 2) || vec.isNullAt(i + 3) ||
          vec.isNullAt(i + 4) || vec.isNullAt(i + 5) ||
          vec.isNullAt(i + 6) || vec.isNullAt(i + 7)) {
        return null;
      }

      result[i] = vec.getFloat(i) / norm;
      result[i + 1] = vec.getFloat(i + 1) / norm;
      result[i + 2] = vec.getFloat(i + 2) / norm;
      result[i + 3] = vec.getFloat(i + 3) / norm;
      result[i + 4] = vec.getFloat(i + 4) / norm;
      result[i + 5] = vec.getFloat(i + 5) / norm;
      result[i + 6] = vec.getFloat(i + 6) / norm;
      result[i + 7] = vec.getFloat(i + 7) / norm;
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      result[i] = vec.getFloat(i) / norm;
      i++;
    }

    return ArrayData.toArrayData(result);
  }

  /**
   * Computes the Lp norm of a float vector using the specified degree.
   * Supported degrees: 1.0 (L1), 2.0 (L2), Float.POSITIVE_INFINITY (L∞).
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws INVALID_VECTOR_NORM_DEGREE if degree is not supported.
   */
  public static Float vectorNorm(ArrayData vec, float degree, UTF8String funcName) {
    // exact floating point comparison for degree since this is direct user input
    if (degree == 1.0f) {
      return vectorL1Norm(vec);
    } else if (degree == 2.0f) {
      return vectorL2Norm(vec);
    } else if (degree == Float.POSITIVE_INFINITY) {
      return vectorInfNorm(vec);
    } else {
      throw QueryExecutionErrors.invalidVectorNormDegreeError(funcName.toString(), degree);
    }
  }

  /**
   * Normalizes a float vector to unit length using the specified norm degree.
   * Supported degrees: 1.0 (L1), 2.0 (L2), Float.POSITIVE_INFINITY (L∞).
   * Returns NULL if the vector contains NULL elements or has zero norm.
   * Returns an empty array for empty vectors.
   * Throws INVALID_VECTOR_NORM_DEGREE if degree is not supported.
   */
  public static ArrayData vectorNormalize(ArrayData vec, float degree, UTF8String funcName) {
    Float norm = vectorNorm(vec, degree, funcName);
    if (norm == null) {
      return null;
    }
    return vectorNormalizeWithNorm(vec, norm);
  }
}
