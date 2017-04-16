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
package org.apache.hadoop.fs.permission;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for parsing either chmod permissions or umask permissions.
 * Includes common code needed by either operation as implemented in
 * UmaskParser and ChmodParser classes.
 */
class PermissionParser {
  protected boolean symbolic = false;
  protected short userMode;
  protected short groupMode;
  protected short othersMode;
  protected short stickyMode;
  protected char userType = '+';
  protected char groupType = '+';
  protected char othersType = '+';
  protected char stickyBitType = '+';
  
  /**
   * Begin parsing permission stored in modeStr
   * 
   * @param modeStr Permission mode, either octal or symbolic
   * @param symbolic Use-case specific symbolic pattern to match against
   * @throws IllegalArgumentException if unable to parse modeStr
   */
  public PermissionParser(String modeStr, Pattern symbolic, Pattern octal) 
       throws IllegalArgumentException {
    Matcher matcher = null;

    if ((matcher = symbolic.matcher(modeStr)).find()) {
      applyNormalPattern(modeStr, matcher);
    } else if ((matcher = octal.matcher(modeStr)).matches()) {
      applyOctalPattern(modeStr, matcher);
    } else {
      throw new IllegalArgumentException(modeStr);
    }
  }

  private void applyNormalPattern(String modeStr, Matcher matcher) {
    // Are there multiple permissions stored in one chmod?
    boolean commaSeperated = false;

    for (int i = 0; i < 1 || matcher.end() < modeStr.length(); i++) {
      if (i > 0 && (!commaSeperated || !matcher.find())) {
        throw new IllegalArgumentException(modeStr);
      }

      /*
       * groups : 1 : [ugoa]* 2 : [+-=] 3 : [rwxXt]+ 4 : [,\s]*
       */

      String str = matcher.group(2);
      char type = str.charAt(str.length() - 1);

      boolean user, group, others, stickyBit;
      user = group = others = stickyBit = false;

      for (char c : matcher.group(1).toCharArray()) {
        switch (c) {
        case 'u':
          user = true;
          break;
        case 'g':
          group = true;
          break;
        case 'o':
          others = true;
          break;
        case 'a':
          break;
        default:
          throw new RuntimeException("Unexpected");
        }
      }

      if (!(user || group || others)) { // same as specifying 'a'
        user = group = others = true;
      }

      short mode = 0;

      for (char c : matcher.group(3).toCharArray()) {
        switch (c) {
        case 'r':
          mode |= 4;
          break;
        case 'w':
          mode |= 2;
          break;
        case 'x':
          mode |= 1;
          break;
        case 'X':
          mode |= 8;
          break;
        case 't':
          stickyBit = true;
          break;
        default:
          throw new RuntimeException("Unexpected");
        }
      }

      if (user) {
        userMode = mode;
        userType = type;
      }

      if (group) {
        groupMode = mode;
        groupType = type;
      }

      if (others) {
        othersMode = mode;
        othersType = type;

        stickyMode = (short) (stickyBit ? 1 : 0);
        stickyBitType = type;
      }

      commaSeperated = matcher.group(4).contains(",");
    }
    symbolic = true;
  }

  private void applyOctalPattern(String modeStr, Matcher matcher) {
    userType = groupType = othersType = '=';

    // Check if sticky bit is specified
    String sb = matcher.group(1);
    if (!sb.isEmpty()) {
      stickyMode = Short.valueOf(sb.substring(0, 1));
      stickyBitType = '=';
    }

    String str = matcher.group(2);
    userMode = Short.valueOf(str.substring(0, 1));
    groupMode = Short.valueOf(str.substring(1, 2));
    othersMode = Short.valueOf(str.substring(2, 3));
  }

  protected int combineModes(int existing, boolean exeOk) {
    return   combineModeSegments(stickyBitType, stickyMode, 
                (existing>>>9), false) << 9 |
             combineModeSegments(userType, userMode,
                (existing>>>6)&7, exeOk) << 6 |
             combineModeSegments(groupType, groupMode,
                (existing>>>3)&7, exeOk) << 3 |
             combineModeSegments(othersType, othersMode, existing&7, exeOk);
  }
  
  protected int combineModeSegments(char type, int mode, 
                                    int existing, boolean exeOk) {
    boolean capX = false;

    if ((mode&8) != 0) { // convert X to x;
      capX = true;
      mode &= ~8;
      mode |= 1;
    }

    switch (type) {
    case '+' : mode = mode | existing; break;
    case '-' : mode = (~mode) & existing; break;
    case '=' : break;
    default  : throw new RuntimeException("Unexpected");      
    }

    // if X is specified add 'x' only if exeOk or x was already set.
    if (capX && !exeOk && (mode&1) != 0 && (existing&1) == 0) {
      mode &= ~1; // remove x
    }

    return mode;
  }
}
