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

/**
 * Contains all the Utils methods used in the masking expressions.
 */
public class MaskExpressionsUtils {
  static final int UNMASKED_VAL = -1;

  /**
   * Returns the masking character for {@param c} or {@param c} is it should not be masked.
   * @param c the character to transform
   * @param maskedUpperChar the character to use instead of a uppercase letter
   * @param maskedLowerChar the character to use instead of a lowercase letter
   * @param maskedDigitChar the character to use instead of a digit
   * @param maskedOtherChar the character to use instead of a any other character
   * @return masking character for {@param c}
   */
  public static int transformChar(
      final int c,
      int maskedUpperChar,
      int maskedLowerChar,
      int maskedDigitChar,
      int maskedOtherChar) {
    switch(Character.getType(c)) {
      case Character.UPPERCASE_LETTER:
        if(maskedUpperChar != UNMASKED_VAL) {
          return maskedUpperChar;
        }
        break;

      case Character.LOWERCASE_LETTER:
        if(maskedLowerChar != UNMASKED_VAL) {
          return maskedLowerChar;
        }
        break;

      case Character.DECIMAL_DIGIT_NUMBER:
        if(maskedDigitChar != UNMASKED_VAL) {
          return maskedDigitChar;
        }
        break;

      default:
        if(maskedOtherChar != UNMASKED_VAL) {
          return maskedOtherChar;
        }
        break;
    }

    return c;
  }

  /**
   * Returns the replacement char to use according to the {@param rep} specified by the user and
   * the {@param def} default.
   */
  public static int getReplacementChar(String rep, int def) {
    if (rep != null && rep.length() > 0) {
      return rep.codePointAt(0);
    }
    return def;
  }
}
