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
#include "hadoop/StringUtils.hh"
#include "hadoop/SerialUtils.hh"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>

using std::string;
using std::vector;

namespace HadoopUtils {

  string toString(int32_t x) {
    char str[100];
    sprintf(str, "%d", x);
    return str;
  }

  int toInt(const string& val) {
    int result;
    char trash;
    int num = sscanf(val.c_str(), "%d%c", &result, &trash);
    HADOOP_ASSERT(num == 1,
                  "Problem converting " + val + " to integer.");
    return result;
  }

  float toFloat(const string& val) {
    float result;
    char trash;
    int num = sscanf(val.c_str(), "%f%c", &result, &trash);
    HADOOP_ASSERT(num == 1,
                  "Problem converting " + val + " to float.");
    return result;
  }

  bool toBool(const string& val) {
    if (val == "true") {
      return true;
    } else if (val == "false") {
      return false;
    } else {
      HADOOP_ASSERT(false,
                    "Problem converting " + val + " to boolean.");
    }
  }

  /**
   * Get the current time in the number of milliseconds since 1970.
   */
  uint64_t getCurrentMillis() {
    struct timeval tv;
    struct timezone tz;
    int sys = gettimeofday(&tv, &tz);
    HADOOP_ASSERT(sys != -1, strerror(errno));
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
  }

  vector<string> splitString(const std::string& str,
			     const char* separator) {
    vector<string> result;
    string::size_type prev_pos=0;
    string::size_type pos=0;
    while ((pos = str.find_first_of(separator, prev_pos)) != string::npos) {
      if (prev_pos < pos) {
	result.push_back(str.substr(prev_pos, pos-prev_pos));
      }
      prev_pos = pos + 1;
    }
    if (prev_pos < str.size()) {
      result.push_back(str.substr(prev_pos));
    }
    return result;
  }

  string quoteString(const string& str,
                     const char* deliminators) {
    
    string result(str);
    for(int i=result.length() -1; i >= 0; --i) {
      char ch = result[i];
      if (!isprint(ch) ||
          ch == '\\' || 
          strchr(deliminators, ch)) {
        switch (ch) {
        case '\\':
          result.replace(i, 1, "\\\\");
          break;
        case '\t':
          result.replace(i, 1, "\\t");
          break;
        case '\n':
          result.replace(i, 1, "\\n");
          break;
        case ' ':
          result.replace(i, 1, "\\s");
          break;
        default:
          char buff[4];
          sprintf(buff, "\\%02x", static_cast<unsigned char>(result[i]));
          result.replace(i, 1, buff);
        }
      }
    }
    return result;
  }

  string unquoteString(const string& str) {
    string result(str);
    string::size_type current = result.find('\\');
    while (current != string::npos) {
      if (current + 1 < result.size()) {
        char new_ch;
        int num_chars;
        if (isxdigit(result[current+1])) {
          num_chars = 2;
          HADOOP_ASSERT(current + num_chars < result.size(),
                     "escape pattern \\<hex><hex> is missing second digit in '"
                     + str + "'");
          char sub_str[3];
          sub_str[0] = result[current+1];
          sub_str[1] = result[current+2];
          sub_str[2] = '\0';
          char* end_ptr = NULL;
          long int int_val = strtol(sub_str, &end_ptr, 16);
          HADOOP_ASSERT(*end_ptr == '\0' && int_val >= 0,
                     "escape pattern \\<hex><hex> is broken in '" + str + "'");
          new_ch = static_cast<char>(int_val);
        } else {
          num_chars = 1;
          switch(result[current+1]) {
          case '\\':
            new_ch = '\\';
            break;
          case 't':
            new_ch = '\t';
            break;
          case 'n':
            new_ch = '\n';
            break;
          case 's':
            new_ch = ' ';
            break;
          default:
            string msg("unknow n escape character '");
            msg += result[current+1];
            HADOOP_ASSERT(false, msg + "' found in '" + str + "'");
          }
        }
        result.replace(current, 1 + num_chars, 1, new_ch);
        current = result.find('\\', current+1);
      } else {
        HADOOP_ASSERT(false, "trailing \\ in '" + str + "'");
      }
    }
    return result;
  }

}
