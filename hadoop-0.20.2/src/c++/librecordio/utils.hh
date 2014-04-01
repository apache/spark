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

#ifndef UTILS_HH_
#define UTILS_HH_

#include "recordio.hh"
#include "typeIDs.hh"


namespace hadoop {

  /**
   * Various utility functions for Hadooop record I/O platform.
   */

class Utils {

private: 
  /** Cannot create a new instance of Utils */
  Utils() {};

public: 

  /**
   * read/skip bytes from stream based on a type
   */
  static void skip(IArchive& a, const char* tag, const TypeID& typeID);

};


}
#endif // UTILS_HH_

