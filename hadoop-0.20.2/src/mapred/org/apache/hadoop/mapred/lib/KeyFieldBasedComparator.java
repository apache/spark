/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.lib;

import java.util.List;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.lib.KeyFieldHelper.KeyDescription;
import org.apache.hadoop.io.Text;

/**
 * This comparator implementation provides a subset of the features provided
 * by the Unix/GNU Sort. In particular, the supported features are:
 * -n, (Sort numerically)
 * -r, (Reverse the result of comparison)
 * -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
 *  of the field to use, and c is the number of the first character from the
 *  beginning of the field. Fields and character posns are numbered starting
 *  with 1; a character position of zero in pos2 indicates the field's last
 *  character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
 *  of the field); if omitted from pos2, it defaults to 0 (the end of the
 *  field). opts are ordering options (any of 'nr' as described above). 
 * We assume that the fields in the key are separated by 
 * map.output.key.field.separator.
 */

public class KeyFieldBasedComparator<K, V> extends WritableComparator 
implements JobConfigurable {
  private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();
  private static final byte NEGATIVE = (byte)'-';
  private static final byte ZERO = (byte)'0';
  private static final byte DECIMAL = (byte)'.';
  
  public void configure(JobConf job) {
    String option = job.getKeyFieldComparatorOption();
    String keyFieldSeparator = job.get("map.output.key.field.separator","\t");
    keyFieldHelper.setKeyFieldSeparator(keyFieldSeparator);
    keyFieldHelper.parseOption(option);
  }
  
  public KeyFieldBasedComparator() {
    super(Text.class);
  }
    

  public int compare(byte[] b1, int s1, int l1,
      byte[] b2, int s2, int l2) {
    int n1 = WritableUtils.decodeVIntSize(b1[s1]);
    int n2 = WritableUtils.decodeVIntSize(b2[s2]);
    List <KeyDescription> allKeySpecs = keyFieldHelper.keySpecs();
    if (allKeySpecs.size() == 0) {
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }
    int []lengthIndicesFirst = keyFieldHelper.getWordLengths(b1, s1+n1, s1+l1);
    int []lengthIndicesSecond = keyFieldHelper.getWordLengths(b2, s2+n2, s2+l2);
    for (KeyDescription keySpec : allKeySpecs) {
      int startCharFirst = keyFieldHelper.getStartOffset(b1, s1+n1, s1+l1, lengthIndicesFirst,
          keySpec);
      int endCharFirst = keyFieldHelper.getEndOffset(b1, s1+n1, s1+l1, lengthIndicesFirst,
          keySpec);
      int startCharSecond = keyFieldHelper.getStartOffset(b2, s2+n2, s2+l2, lengthIndicesSecond,
          keySpec);
      int endCharSecond = keyFieldHelper.getEndOffset(b2, s2+n2, s2+l2, lengthIndicesSecond,
          keySpec);
      int result;
      if ((result = compareByteSequence(b1, startCharFirst, endCharFirst, b2, 
          startCharSecond, endCharSecond, keySpec)) != 0) {
        return result;
      }
    }
    return 0;
  }
  
  private int compareByteSequence(byte[] first, int start1, int end1, 
      byte[] second, int start2, int end2, KeyDescription key) {
    if (start1 == -1) {
      if (key.reverse) {
        return 1;
      }
      return -1;
    }
    if (start2 == -1) {
      if (key.reverse) {
        return -1; 
      }
      return 1;
    }
    int compareResult = 0;
    if (!key.numeric) {
      compareResult = compareBytes(first, start1, end1-start1+1, second, start2, end2-start2+1);
    }
    if (key.numeric) {
      compareResult = numericalCompare (first, start1, end1, second, start2, end2);
    }
    if (key.reverse) {
      return -compareResult;
    }
    return compareResult;
  }
  
  private int numericalCompare (byte[] a, int start1, int end1, 
      byte[] b, int start2, int end2) {
    int i = start1;
    int j = start2;
    int mul = 1;
    byte first_a = a[i];
    byte first_b = b[j];
    if (first_a == NEGATIVE) {
      if (first_b != NEGATIVE) {
        //check for cases like -0.0 and 0.0 (they should be declared equal)
        return oneNegativeCompare(a,start1+1,end1,b,start2,end2);
      }
      i++;
    }
    if (first_b == NEGATIVE) {
      if (first_a != NEGATIVE) {
        //check for cases like 0.0 and -0.0 (they should be declared equal)
        return -oneNegativeCompare(b,start2+1,end2,a,start1,end1);
      }
      j++;
    }
    if (first_b == NEGATIVE && first_a == NEGATIVE) {
      mul = -1;
    }

    //skip over ZEROs
    while (i <= end1) {
      if (a[i] != ZERO) {
        break;
      }
      i++;
    }
    while (j <= end2) {
      if (b[j] != ZERO) {
        break;
      }
      j++;
    }
    
    //skip over equal characters and stopping at the first nondigit char
    //The nondigit character could be '.'
    while (i <= end1 && j <= end2) {
      if (!isdigit(a[i]) || a[i] != b[j]) {
        break;
      }
      i++; j++;
    }
    if (i <= end1) {
      first_a = a[i];
    }
    if (j <= end2) {
      first_b = b[j];
    }
    //store the result of the difference. This could be final result if the
    //number of digits in the mantissa is the same in both the numbers 
    int firstResult = first_a - first_b;
    
    //check whether we hit a decimal in the earlier scan
    if ((first_a == DECIMAL && (!isdigit(first_b) || j > end2)) ||
            (first_b == DECIMAL && (!isdigit(first_a) || i > end1))) {
      return ((mul < 0) ? -decimalCompare(a,i,end1,b,j,end2) : 
        decimalCompare(a,i,end1,b,j,end2));
    }
    //check the number of digits in the mantissa of the numbers
    int numRemainDigits_a = 0;
    int numRemainDigits_b = 0;
    while (i <= end1) {
      //if we encounter a non-digit treat the corresponding number as being 
      //smaller      
      if (isdigit(a[i++])) {
        numRemainDigits_a++;
      } else break;
    }
    while (j <= end2) {
      //if we encounter a non-digit treat the corresponding number as being 
      //smaller
      if (isdigit(b[j++])) {
        numRemainDigits_b++;
      } else break;
    }
    int ret = numRemainDigits_a - numRemainDigits_b;
    if (ret == 0) { 
      return ((mul < 0) ? -firstResult : firstResult);
    } else {
      return ((mul < 0) ? -ret : ret);
    }
  }
  private boolean isdigit(byte b) {
    if ('0' <= b && b <= '9') {
      return true;
    }
    return false;
  }
  private int decimalCompare(byte[] a, int i, int end1, 
                             byte[] b, int j, int end2) {
    if (i > end1) {
      //if a[] has nothing remaining
      return -decimalCompare1(b, ++j, end2);
    }
    if (j > end2) {
      //if b[] has nothing remaining
      return decimalCompare1(a, ++i, end1);
    }
    if (a[i] == DECIMAL && b[j] == DECIMAL) {
      while (i <= end1 && j <= end2) {
        if (a[i] != b[j]) {
          if (isdigit(a[i]) && isdigit(b[j])) {
            return a[i] - b[j];
          }
          if (isdigit(a[i])) {
            return 1;
          }
          if (isdigit(b[j])) {
            return -1;
          }
          return 0;
        }
        i++; j++;
      }
      if (i > end1 && j > end2) {
        return 0;
      }
        
      if (i > end1) {
        //check whether there is a non-ZERO digit after potentially
        //a number of ZEROs (e.g., a=.4444, b=.444400004)
        return -decimalCompare1(b, j, end2);
      }
      if (j > end2) {
        //check whether there is a non-ZERO digit after potentially
        //a number of ZEROs (e.g., b=.4444, a=.444400004)
        return decimalCompare1(a, i, end1);
      }
    }
    else if (a[i] == DECIMAL) {
      return decimalCompare1(a, ++i, end1);
    }
    else if (b[j] == DECIMAL) {
      return -decimalCompare1(b, ++j, end2);
    }
    return 0;
  }
  
  private int decimalCompare1(byte[] a, int i, int end) {
    while (i <= end) {
      if (a[i] == ZERO) {
        i++;
        continue;
      }
      if (isdigit(a[i])) {
        return 1;
      } else {
        return 0;
      }
    }
    return 0;
  }
  
  private int oneNegativeCompare(byte[] a, int start1, int end1, 
      byte[] b, int start2, int end2) {
    //here a[] is negative and b[] is positive
    //We have to ascertain whether the number contains any digits.
    //If it does, then it is a smaller number for sure. If not,
    //then we need to scan b[] to find out whether b[] has a digit
    //If b[] does contain a digit, then b[] is certainly
    //greater. If not, that is, both a[] and b[] don't contain
    //digits then they should be considered equal.
    if (!isZero(a, start1, end1)) {
      return -1;
    }
    //reached here - this means that a[] is a ZERO
    if (!isZero(b, start2, end2)) {
      return -1;
    }
    //reached here - both numbers are basically ZEROs and hence
    //they should compare equal
    return 0;
  }
  
  private boolean isZero(byte a[], int start, int end) {
    //check for zeros in the significand part as well as the decimal part
    //note that we treat the non-digit characters as ZERO
    int i = start;
    //we check the significand for being a ZERO
    while (i <= end) {
      if (a[i] != ZERO) {
        if (a[i] != DECIMAL && isdigit(a[i])) {
          return false;
        }
        break;
      }
      i++;
    }

    if (i != (end+1) && a[i++] == DECIMAL) {
      //we check the decimal part for being a ZERO
      while (i <= end) {
        if (a[i] != ZERO) {
          if (isdigit(a[i])) {
            return false;
          }
          break;
        }
        i++;
      }
    }
    return true;
  }
}
