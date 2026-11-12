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
   * The dot product and the squared norms are accumulated in double precision: their magnitudes
   * are quadratic in the input values, so single precision would overflow to infinity (or
   * underflow to zero) for vectors whose cosine similarity is perfectly representable as a float.
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

    double dotProduct = 0.0d;
    double norm1Sq = 0.0d;
    double norm2Sq = 0.0d;

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

      double a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      double a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      double a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      double a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      double b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      double b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      double b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      double b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

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
      double a = left.getFloat(i);
      double b = right.getFloat(i);
      dotProduct += a * b;
      norm1Sq += a * a;
      norm2Sq += b * b;
      i++;
    }

    // For vectors of finite elements, `norm1Sq * norm2Sq` cannot overflow in double precision:
    // both factors are bounded by MAX_ROUNDED_ARRAY_LENGTH * Float.MAX_VALUE^2, so their product
    // stays well below Double.MAX_VALUE. An element that is already infinite makes the product
    // infinite and the result NaN, exactly as it did before the accumulators were widened.
    double normProduct = Math.sqrt(norm1Sq * norm2Sq);
    if (normProduct == 0.0d) {
      return null;
    }
    return (float) (dotProduct / normProduct);
  }

  /**
   * Computes the inner product (dot product) between two float vectors.
   * Returns NULL if either vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws an exception if vectors have different dimensions.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   * The dot product is accumulated in double precision so that intermediate terms do not
   * overflow to infinity when the final result is representable as a float.
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

    double dotProduct = 0.0d;

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

      double a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      double a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      double a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      double a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      double b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      double b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      double b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      double b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

      dotProduct += a0 * b0 + a1 * b1 + a2 * b2 + a3 * b3 +
                    a4 * b4 + a5 * b5 + a6 * b6 + a7 * b7;
      i += 8;
    }

    // Handle remaining elements
    while (i < leftLen) {
      if (left.isNullAt(i) || right.isNullAt(i)) {
        return null;
      }
      double a = left.getFloat(i);
      double b = right.getFloat(i);
      dotProduct += a * b;
      i++;
    }

    return (float) dotProduct;
  }

  /**
   * Computes the Euclidean (L2) distance between two float vectors.
   * Returns NULL if either vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws an exception if vectors have different dimensions.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   * The sum of squares is accumulated in double precision: it is quadratic in the input values,
   * so single precision would overflow to infinity for distances representable as a float.
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

    double sumSq = 0.0d;

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

      double a0 = left.getFloat(i), a1 = left.getFloat(i + 1);
      double a2 = left.getFloat(i + 2), a3 = left.getFloat(i + 3);
      double a4 = left.getFloat(i + 4), a5 = left.getFloat(i + 5);
      double a6 = left.getFloat(i + 6), a7 = left.getFloat(i + 7);

      double b0 = right.getFloat(i), b1 = right.getFloat(i + 1);
      double b2 = right.getFloat(i + 2), b3 = right.getFloat(i + 3);
      double b4 = right.getFloat(i + 4), b5 = right.getFloat(i + 5);
      double b6 = right.getFloat(i + 6), b7 = right.getFloat(i + 7);

      double d0 = a0 - b0, d1 = a1 - b1, d2 = a2 - b2, d3 = a3 - b3;
      double d4 = a4 - b4, d5 = a5 - b5, d6 = a6 - b6, d7 = a7 - b7;

      sumSq += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3 +
               d4 * d4 + d5 * d5 + d6 * d6 + d7 * d7;
      i += 8;
    }

    // Handle remaining elements
    while (i < leftLen) {
      if (left.isNullAt(i) || right.isNullAt(i)) {
        return null;
      }
      double a = left.getFloat(i);
      double b = right.getFloat(i);
      double diff = a - b;
      sumSq += diff * diff;
      i++;
    }

    return (float) Math.sqrt(sumSq);
  }

  /**
   * Computes the L1 norm (Manhattan norm) of a float vector, in double precision.
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Double vectorL1Norm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0d;
    }

    double sum = 0.0d;

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

      double a0 = vec.getFloat(i), a1 = vec.getFloat(i + 1);
      double a2 = vec.getFloat(i + 2), a3 = vec.getFloat(i + 3);
      double a4 = vec.getFloat(i + 4), a5 = vec.getFloat(i + 5);
      double a6 = vec.getFloat(i + 6), a7 = vec.getFloat(i + 7);

      sum += Math.abs(a0) + Math.abs(a1) + Math.abs(a2) + Math.abs(a3) +
             Math.abs(a4) + Math.abs(a5) + Math.abs(a6) + Math.abs(a7);
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      double a = vec.getFloat(i);
      sum += Math.abs(a);
      i++;
    }

    return sum;
  }

  /**
   * Computes the L2 norm (Euclidean norm) of a float vector, in double precision.
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   */
  public static Double vectorL2Norm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0d;
    }

    double sumSq = 0.0d;

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

      double a0 = vec.getFloat(i), a1 = vec.getFloat(i + 1);
      double a2 = vec.getFloat(i + 2), a3 = vec.getFloat(i + 3);
      double a4 = vec.getFloat(i + 4), a5 = vec.getFloat(i + 5);
      double a6 = vec.getFloat(i + 6), a7 = vec.getFloat(i + 7);

      sumSq += a0 * a0 + a1 * a1 + a2 * a2 + a3 * a3 +
               a4 * a4 + a5 * a5 + a6 * a6 + a7 * a7;
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      double a = vec.getFloat(i);
      sumSq += a * a;
      i++;
    }

    return Math.sqrt(sumSq);
  }

  /**
   * Computes the infinity norm (maximum absolute value) of a float vector, in double precision.
   * Returns NULL if the vector contains NULL elements.
   * Returns NaN if the vector contains NaN elements, following the convention of max and
   * array_max that NaN compares as larger than any other value.
   * Returns 0.0 for empty vectors.
   */
  public static Double vectorInfNorm(ArrayData vec) {
    int len = vec.numElements();

    if (len == 0) {
      return 0.0d;
    }

    // Math.max is used rather than a comparison against the running maximum: every comparison
    // involving NaN is false, so a hand-rolled maximum would skip NaN elements and report the
    // largest of the remaining ones instead of propagating the NaN.
    double maxAbs = 0.0d;
    for (int i = 0; i < len; i++) {
      if (vec.isNullAt(i)) {
        return null;
      }
      maxAbs = Math.max(maxAbs, Math.abs((double) vec.getFloat(i)));
    }

    return maxAbs;
  }

  /**
   * Normalizes a float vector by dividing each element by the given norm.
   * Returns NULL if the vector contains NULL elements or if the norm is zero.
   * Returns an empty array for empty vectors.
   * Uses manual loop unrolling (8 elements at a time) for speculative SIMD optimization.
   * The norm is taken in double precision so that vectors whose norm is not representable as a
   * float (or is only representable as a subnormal float) are still normalized correctly.
   */
  public static ArrayData vectorNormalizeWithNorm(ArrayData vec, double norm) {
    int len = vec.numElements();

    if (len == 0) {
      return vec;
    }

    if (norm == 0.0d) {
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

      result[i] = (float) (vec.getFloat(i) / norm);
      result[i + 1] = (float) (vec.getFloat(i + 1) / norm);
      result[i + 2] = (float) (vec.getFloat(i + 2) / norm);
      result[i + 3] = (float) (vec.getFloat(i + 3) / norm);
      result[i + 4] = (float) (vec.getFloat(i + 4) / norm);
      result[i + 5] = (float) (vec.getFloat(i + 5) / norm);
      result[i + 6] = (float) (vec.getFloat(i + 6) / norm);
      result[i + 7] = (float) (vec.getFloat(i + 7) / norm);
      i += 8;
    }

    // Handle remaining elements
    while (i < len) {
      if (vec.isNullAt(i)) {
        return null;
      }
      result[i] = (float) (vec.getFloat(i) / norm);
      i++;
    }

    return ArrayData.toArrayData(result);
  }

  /**
   * Computes the Lp norm of a float vector using the specified degree, in double precision.
   * Supported degrees: 1.0 (L1), 2.0 (L2), Float.POSITIVE_INFINITY (infinity norm).
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws INVALID_VECTOR_NORM_DEGREE if degree is not supported.
   */
  private static Double vectorNormAsDouble(ArrayData vec, float degree, UTF8String funcName) {
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
   * Computes the Lp norm of a float vector using the specified degree.
   * Supported degrees: 1.0 (L1), 2.0 (L2), Float.POSITIVE_INFINITY (infinity norm).
   * Returns NULL if the vector contains NULL elements.
   * Returns 0.0 for empty vectors.
   * Throws INVALID_VECTOR_NORM_DEGREE if degree is not supported.
   */
  public static Float vectorNorm(ArrayData vec, float degree, UTF8String funcName) {
    Double norm = vectorNormAsDouble(vec, degree, funcName);
    if (norm == null) {
      return null;
    }
    return (float) norm.doubleValue();
  }

  /**
   * Normalizes a float vector to unit length using the specified norm degree.
   * Supported degrees: 1.0 (L1), 2.0 (L2), Float.POSITIVE_INFINITY (infinity norm).
   * Returns NULL if the vector contains NULL elements or has zero norm.
   * Returns an empty array for empty vectors.
   * Throws INVALID_VECTOR_NORM_DEGREE if degree is not supported.
   */
  public static ArrayData vectorNormalize(ArrayData vec, float degree, UTF8String funcName) {
    // The norm is kept in double precision here: rounding it to a float first would turn a norm
    // that overflows (or underflows) the float range into infinity (or zero) and produce an
    // all-zero (or NULL) result for a vector that is perfectly normalizable.
    Double norm = vectorNormAsDouble(vec, degree, funcName);
    if (norm == null) {
      return null;
    }
    return vectorNormalizeWithNorm(vec, norm);
  }
}
