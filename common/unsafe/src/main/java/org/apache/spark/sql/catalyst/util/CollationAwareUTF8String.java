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
package org.apache.spark.sql.catalyst.util;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.StringSearch;
import com.ibm.icu.util.ULocale;

import org.apache.spark.unsafe.UTF8StringBuilder;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import static org.apache.spark.unsafe.Platform.copyMemory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for collation-aware UTF8String operations.
 */
public class CollationAwareUTF8String {
  public static UTF8String replace(final UTF8String src, final UTF8String search,
      final UTF8String replace, final int collationId) {
    // This collation aware implementation is based on existing implementation on UTF8String
    if (src.numBytes() == 0 || search.numBytes() == 0) {
      return src;
    }

    StringSearch stringSearch = CollationFactory.getStringSearch(src, search, collationId);

    // Find the first occurrence of the search string.
    int end = stringSearch.next();
    if (end == StringSearch.DONE) {
      // Search string was not found, so string is unchanged.
      return src;
    }

    // Initialize byte positions
    int c = 0;
    int byteStart = 0; // position in byte
    int byteEnd = 0; // position in byte
    while (byteEnd < src.numBytes() && c < end) {
      byteEnd += UTF8String.numBytesForFirstByte(src.getByte(byteEnd));
      c += 1;
    }

    // At least one match was found. Estimate space needed for result.
    // The 16x multiplier here is chosen to match commons-lang3's implementation.
    int increase = Math.max(0, Math.abs(replace.numBytes() - search.numBytes())) * 16;
    final UTF8StringBuilder buf = new UTF8StringBuilder(src.numBytes() + increase);
    while (end != StringSearch.DONE) {
      buf.appendBytes(src.getBaseObject(), src.getBaseOffset() + byteStart, byteEnd - byteStart);
      buf.append(replace);

      // Move byteStart to the beginning of the current match
      byteStart = byteEnd;
      int cs = c;
      // Move cs to the end of the current match
      // This is necessary because the search string may contain 'multi-character' characters
      while (byteStart < src.numBytes() && cs < c + stringSearch.getMatchLength()) {
        byteStart += UTF8String.numBytesForFirstByte(src.getByte(byteStart));
        cs += 1;
      }
      // Go to next match
      end = stringSearch.next();
      // Update byte positions
      while (byteEnd < src.numBytes() && c < end) {
        byteEnd += UTF8String.numBytesForFirstByte(src.getByte(byteEnd));
        c += 1;
      }
    }
    buf.appendBytes(src.getBaseObject(), src.getBaseOffset() + byteStart,
      src.numBytes() - byteStart);
    return buf.build();
  }

  public static UTF8String lowercaseReplace(final UTF8String src, final UTF8String search,
      final UTF8String replace) {
    if (src.numBytes() == 0 || search.numBytes() == 0) {
      return src;
    }
    UTF8String lowercaseString = src.toLowerCase();
    UTF8String lowercaseSearch = search.toLowerCase();

    int start = 0;
    int end = lowercaseString.indexOf(lowercaseSearch, 0);
    if (end == -1) {
      // Search string was not found, so string is unchanged.
      return src;
    }

    // Initialize byte positions
    int c = 0;
    int byteStart = 0; // position in byte
    int byteEnd = 0; // position in byte
    while (byteEnd < src.numBytes() && c < end) {
      byteEnd += UTF8String.numBytesForFirstByte(src.getByte(byteEnd));
      c += 1;
    }

    // At least one match was found. Estimate space needed for result.
    // The 16x multiplier here is chosen to match commons-lang3's implementation.
    int increase = Math.max(0, replace.numBytes() - search.numBytes()) * 16;
    final UTF8StringBuilder buf = new UTF8StringBuilder(src.numBytes() + increase);
    while (end != -1) {
      buf.appendBytes(src.getBaseObject(), src.getBaseOffset() + byteStart, byteEnd - byteStart);
      buf.append(replace);
      // Update character positions
      start = end + lowercaseSearch.numChars();
      end = lowercaseString.indexOf(lowercaseSearch, start);
      // Update byte positions
      byteStart = byteEnd + search.numBytes();
      while (byteEnd < src.numBytes() && c < end) {
        byteEnd += UTF8String.numBytesForFirstByte(src.getByte(byteEnd));
        c += 1;
      }
    }
    buf.appendBytes(src.getBaseObject(), src.getBaseOffset() + byteStart,
      src.numBytes() - byteStart);
    return buf.build();
  }

  public static String toUpperCase(final String target, final int collationId) {
    ULocale locale = CollationFactory.fetchCollation(collationId)
      .collator.getLocale(ULocale.ACTUAL_LOCALE);
    return UCharacter.toUpperCase(locale, target);
  }

  public static String toLowerCase(final String target, final int collationId) {
    ULocale locale = CollationFactory.fetchCollation(collationId)
      .collator.getLocale(ULocale.ACTUAL_LOCALE);
    return UCharacter.toLowerCase(locale, target);
  }

  public static String toTitleCase(final String target, final int collationId) {
    ULocale locale = CollationFactory.fetchCollation(collationId)
      .collator.getLocale(ULocale.ACTUAL_LOCALE);
    return UCharacter.toTitleCase(locale, target, BreakIterator.getWordInstance(locale));
  }

  public static int findInSet(final UTF8String match, final UTF8String set, int collationId) {
    if (match.contains(UTF8String.fromString(","))) {
      return 0;
    }

    String setString = set.toString();
    StringSearch stringSearch = CollationFactory.getStringSearch(setString, match.toString(),
      collationId);

    int wordStart = 0;
    while ((wordStart = stringSearch.next()) != StringSearch.DONE) {
      boolean isValidStart = wordStart == 0 || setString.charAt(wordStart - 1) == ',';
      boolean isValidEnd = wordStart + stringSearch.getMatchLength() == setString.length()
        || setString.charAt(wordStart + stringSearch.getMatchLength()) == ',';

      if (isValidStart && isValidEnd) {
        int pos = 0;
        for (int i = 0; i < setString.length() && i < wordStart; i++) {
          if (setString.charAt(i) == ',') {
            pos++;
          }
        }

        return pos + 1;
      }
    }

    return 0;
  }

  public static int indexOf(final UTF8String target, final UTF8String pattern,
      final int start, final int collationId) {
    if (pattern.numBytes() == 0) {
      return 0;
    }

    StringSearch stringSearch = CollationFactory.getStringSearch(target, pattern, collationId);
    stringSearch.setIndex(start);

    return stringSearch.next();
  }

  public static int find(UTF8String target, UTF8String pattern, int start,
      int collationId) {
    assert (pattern.numBytes() > 0);

    StringSearch stringSearch = CollationFactory.getStringSearch(target, pattern, collationId);
    // Set search start position (start from character at start position)
    stringSearch.setIndex(target.bytePosToChar(start));

    // Return either the byte position or -1 if not found
    return target.charPosToByte(stringSearch.next());
  }

  public static UTF8String subStringIndex(final UTF8String string, final UTF8String delimiter,
      int count, final int collationId) {
    if (delimiter.numBytes() == 0 || count == 0 || string.numBytes() == 0) {
      return UTF8String.EMPTY_UTF8;
    }
    if (count > 0) {
      int idx = -1;
      while (count > 0) {
        idx = find(string, delimiter, idx + 1, collationId);
        if (idx >= 0) {
          count --;
        } else {
          // can not find enough delim
          return string;
        }
      }
      if (idx == 0) {
        return UTF8String.EMPTY_UTF8;
      }
      byte[] bytes = new byte[idx];
      copyMemory(string.getBaseObject(), string.getBaseOffset(), bytes, BYTE_ARRAY_OFFSET, idx);
      return UTF8String.fromBytes(bytes);

    } else {
      count = -count;

      StringSearch stringSearch = CollationFactory
        .getStringSearch(string, delimiter, collationId);

      int start = string.numChars() - 1;
      int lastMatchLength = 0;
      int prevStart = -1;
      while (count > 0) {
        stringSearch.reset();
        prevStart = -1;
        int matchStart = stringSearch.next();
        lastMatchLength = stringSearch.getMatchLength();
        while (matchStart <= start) {
          if (matchStart != StringSearch.DONE) {
            // Found a match, update the start position
            prevStart = matchStart;
            matchStart = stringSearch.next();
          } else {
            break;
          }
        }

        if (prevStart == -1) {
          // can not find enough delim
          return string;
        } else {
          start = prevStart - 1;
          count--;
        }
      }

      int resultStart = prevStart + lastMatchLength;
      if (resultStart == string.numChars()) {
        return UTF8String.EMPTY_UTF8;
      }

      return string.substring(resultStart, string.numChars());
    }
  }

  public static UTF8String lowercaseSubStringIndex(final UTF8String string,
      final UTF8String delimiter, int count) {
    if (delimiter.numBytes() == 0 || count == 0) {
      return UTF8String.EMPTY_UTF8;
    }

    UTF8String lowercaseString = string.toLowerCase();
    UTF8String lowercaseDelimiter = delimiter.toLowerCase();

    if (count > 0) {
      int idx = -1;
      while (count > 0) {
        idx = lowercaseString.find(lowercaseDelimiter, idx + 1);
        if (idx >= 0) {
          count--;
        } else {
          // can not find enough delim
          return string;
        }
      }
      if (idx == 0) {
        return UTF8String.EMPTY_UTF8;
      }
      byte[] bytes = new byte[idx];
      copyMemory(string.getBaseObject(), string.getBaseOffset(), bytes, BYTE_ARRAY_OFFSET, idx);
      return UTF8String.fromBytes(bytes);

    } else {
      int idx = string.numBytes() - delimiter.numBytes() + 1;
      count = -count;
      while (count > 0) {
        idx = lowercaseString.rfind(lowercaseDelimiter, idx - 1);
        if (idx >= 0) {
          count--;
        } else {
          // can not find enough delim
          return string;
        }
      }
      if (idx + delimiter.numBytes() == string.numBytes()) {
        return UTF8String.EMPTY_UTF8;
      }
      int size = string.numBytes() - delimiter.numBytes() - idx;
      byte[] bytes = new byte[size];
      copyMemory(string.getBaseObject(), string.getBaseOffset() + idx + delimiter.numBytes(),
        bytes, BYTE_ARRAY_OFFSET, size);
      return UTF8String.fromBytes(bytes);
    }
  }

  public static Map<String, String> getCollationAwareDict(UTF8String string,
      Map<String, String> dict, int collationId) {
    String srcStr = string.toString();

    Map<String, String> collationAwareDict = new HashMap<>();
    for (String key : dict.keySet()) {
      StringSearch stringSearch =
        CollationFactory.getStringSearch(string, UTF8String.fromString(key), collationId);

      int pos = 0;
      while ((pos = stringSearch.next()) != StringSearch.DONE) {
        int codePoint = srcStr.codePointAt(pos);
        int charCount = Character.charCount(codePoint);
        String newKey = srcStr.substring(pos, pos + charCount);

        boolean exists = false;
        for (String existingKey : collationAwareDict.keySet()) {
          if (stringSearch.getCollator().compare(existingKey, newKey) == 0) {
            collationAwareDict.put(newKey, collationAwareDict.get(existingKey));
            exists = true;
            break;
          }
        }

        if (!exists) {
          collationAwareDict.put(newKey, dict.get(key));
        }
      }
    }

    return collationAwareDict;
  }

  public static UTF8String lowercaseTrim(
      final UTF8String srcString,
      final UTF8String trimString) {
    // Matching UTF8String behavior for null `trimString`.
    if (trimString == null) {
      return null;
    }

    UTF8String leftTrimmed = lowercaseTrimLeft(srcString, trimString);
    return lowercaseTrimRight(leftTrimmed, trimString);
  }

  public static UTF8String lowercaseTrimLeft(
      final UTF8String srcString,
      final UTF8String trimString) {
    // Matching UTF8String behavior for null `trimString`.
    if (trimString == null) {
      return null;
    }

    // The searching byte position in the srcString.
    int searchIdx = 0;
    // The byte position of a first non-matching character in the srcString.
    int trimByteIdx = 0;
    // Number of bytes in srcString.
    int numBytes = srcString.numBytes();
    // Convert trimString to lowercase, so it can be searched properly.
    UTF8String lowercaseTrimString = trimString.toLowerCase();

    while (searchIdx < numBytes) {
      UTF8String searchChar = srcString.copyUTF8String(
        searchIdx,
        searchIdx + UTF8String.numBytesForFirstByte(srcString.getByte(searchIdx)) - 1);
      int searchCharBytes = searchChar.numBytes();

      // Try to find the matching for the searchChar in the trimString.
      if (lowercaseTrimString.find(searchChar.toLowerCase(), 0) >= 0) {
        trimByteIdx += searchCharBytes;
        searchIdx += searchCharBytes;
      } else {
        // No matching, exit the search.
        break;
      }
    }

    if (searchIdx == 0) {
      // Nothing trimmed - return original string (not converted to lowercase).
      return srcString;
    }
    if (trimByteIdx >= numBytes) {
      // Everything trimmed.
      return UTF8String.EMPTY_UTF8;
    }
    return srcString.copyUTF8String(trimByteIdx, numBytes - 1);
  }

  public static UTF8String lowercaseTrimRight(
      final UTF8String srcString,
      final UTF8String trimString) {
    // Matching UTF8String behavior for null `trimString`.
    if (trimString == null) {
      return null;
    }

    // Number of bytes iterated from the srcString.
    int byteIdx = 0;
    // Number of characters iterated from the srcString.
    int numChars = 0;
    // Number of bytes in srcString.
    int numBytes = srcString.numBytes();
    // Array of character length for the srcString.
    int[] stringCharLen = new int[numBytes];
    // Array of the first byte position for each character in the srcString.
    int[] stringCharPos = new int[numBytes];
    // Convert trimString to lowercase, so it can be searched properly.
    UTF8String lowercaseTrimString = trimString.toLowerCase();

    // Build the position and length array.
    while (byteIdx < numBytes) {
      stringCharPos[numChars] = byteIdx;
      stringCharLen[numChars] = UTF8String.numBytesForFirstByte(srcString.getByte(byteIdx));
      byteIdx += stringCharLen[numChars];
      numChars++;
    }

    // Index trimEnd points to the first no matching byte position from the right side of
    //  the source string.
    int trimByteIdx = numBytes - 1;

    while (numChars > 0) {
      UTF8String searchChar = srcString.copyUTF8String(
        stringCharPos[numChars - 1],
        stringCharPos[numChars - 1] + stringCharLen[numChars - 1] - 1);

      if(lowercaseTrimString.find(searchChar.toLowerCase(), 0) >= 0) {
        trimByteIdx -= stringCharLen[numChars - 1];
        numChars--;
      } else {
        break;
      }
    }

    if (trimByteIdx == numBytes - 1) {
      // Nothing trimmed.
      return srcString;
    }
    if (trimByteIdx < 0) {
      // Everything trimmed.
      return UTF8String.EMPTY_UTF8;
    }
    return srcString.copyUTF8String(0, trimByteIdx);
  }
}
