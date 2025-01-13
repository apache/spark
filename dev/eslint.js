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

module.exports = {
  "env": {
    "browser": true,
    "es6": true,
    "jest": true
  },
  "extends": "eslint:recommended",
  "rules": {
    "indent": [
      "error",
      2,
      {
        "SwitchCase": 1,
        "MemberExpression": "off"
        }
    ],
    "no-unused-vars": ["error", {"argsIgnorePattern": "^_ignored_.*"}]
  },
  "ignorePatterns": [
    "*.min.js",
    "sorttable.js",
    "jquery.mustache.js",
    "dataTables.rowsGroup.js"
  ],
  "parserOptions": {
    "sourceType": "module"
  }
}
