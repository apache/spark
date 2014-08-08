#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SPARK_HOME="$(cd `dirname $0`/..; pwd)"
PROPERTIES_FILE="$SPARK_HOME/bin/test.conf"

# Load utility functions
. "$SPARK_HOME/bin/utils.sh"

tests_failed=0

# Test parse_java_property. This takes in three parameters, the name of the config,
# the expected value, and whether or not to ignore whitespace (e.g. for multi-line).
test_parse_java_property() {
  key="$1"
  expected_value="$2"
  ignore_whitespace="$3"
  parse_java_property "$key"
  actual_value="$JAVA_PROPERTY_VALUE"
  echo "  $key -> $actual_value"
  # Ignore whitespace for multi-line arguments
  if [[ -n "$ignore_whitespace" ]]; then
    expected_value=$(echo "$expected_value" | sed "s/[[:space:]]//g")
    actual_value=$(echo "$actual_value" | sed "s/[[:space:]]//g")
  fi
  if [[ "$actual_value" != "$expected_value" ]]; then
    echo "    XXXXX TEST FAILED XXXXX"
    echo "      expected: $expected_value"
    echo "      actual:   $actual_value"
    tests_failed=1
  fi
}

# Test split_java_options. This takes in three or more parameters, the name of the config,
# the expected number of java options, and values of the java options themselves.
test_split_java_options() {
  key="$1"
  expected_size="$2"
  expected_values=("${@:3}")
  parse_java_property "$key"
  echo "  $JAVA_PROPERTY_VALUE"
  split_java_options "$JAVA_PROPERTY_VALUE"
  if [[ "$expected_size" != "${#SPLIT_JAVA_OPTS[@]}" ]]; then
    echo "    XXXXX TEST FAILED XXXXX"
    echo "      expected size: $expected_size"
    echo "      actual size:   ${#SPLIT_JAVA_OPTS[@]}"
  fi
  for i in $(seq 0 $((expected_size - 1))); do
    expected_value="${expected_values[$i]}"
    actual_value="${SPLIT_JAVA_OPTS[$i]}"
    echo "    -> $actual_value"
    if [[ "$expected_value" != "$actual_value" ]]; then
      echo "      XXXXX TEST FAILED (key $key) XXXXX"
      echo "        expected value: $expected_value"
      echo "        actual value:   $actual_value"
      tests_failed=1
      break
    fi
  done
}

# Test split_java_options. This takes in three or more parameters, the name of the config,
# the expected number of java options, and values of the java options themselves.
test_quote_java_property() {
  key="$1"
  expected_size="$2"
  expected_values=("${@:3}")
  parse_java_property "$key"
  split_java_options "$JAVA_PROPERTY_VALUE"
  quote_java_property "${SPLIT_JAVA_OPTS[@]}"
  echo "  $JAVA_PROPERTY_VALUE"
  for i in $(seq 0 $((expected_size - 1))); do
    expected_value="${expected_values[$i]}"
    actual_value="${QUOTED_JAVA_OPTS[$i]}"
    echo "    -> $actual_value"
    if [[ "$expected_value" != "$actual_value" ]]; then
      echo "      XXXXX TEST FAILED (key $key) XXXXX"
      echo "        expected value: $expected_value"
      echo "        actual value:   $actual_value"
      tests_failed=1
      break
    fi
  done
}

# Test parse_java_property. This should read the literal value as written in the conf file.
echo "--- Testing parse_java_property ---"
delimiters=("space" "equal" "colon")
test_parse_java_property "does.not.exist" ""
for delimiter in "${delimiters[@]}"; do
  test_parse_java_property "spark.$delimiter.1" "-Dstraw=berry"
  test_parse_java_property "spark.$delimiter.2" "-Dstraw=\"berry\""
  test_parse_java_property "spark.$delimiter.3" "-Dstraw=\"berry again\""
  test_parse_java_property "spark.$delimiter.4" "-Dstraw=\"berry \\\"quote\""
  test_parse_java_property "spark.$delimiter.5" "-Dstraw=\"berry \\\\backslash\""
  test_parse_java_property "spark.$delimiter.6" "-Dstraw=\"berry \\\"quotes\\\" and \\\\backslashes\\\\ \""
  test_parse_java_property "spark.$delimiter.7" "-Dstraw=berry -Dblue=berry -Dblack=berry"
  test_parse_java_property "spark.$delimiter.8" "-Dstraw=\"berry space\" -Dblue=\"berry\" -Dblack=berry"
  test_parse_java_property "spark.$delimiter.9" \
    "-Dstraw=\"berry space\" -Dblue=\"berry \\\"quotes\\\"\" -Dblack=\"berry \\\\backslashes\\\\ \""
  test_parse_java_property "spark.$delimiter.10" \
    "-Dstraw=\"berry space\" -Dblue=\"berry \\\"quotes\\\"\" -Dblack=\"berry \\\\backslashes\\\\ \"" IGNORE_WHITESPACE
  test_parse_java_property "spark.$delimiter.11" \
    "-Dstraw=\"berry space\" -Dblue=\"berry \\\"quotes\\\"\" -Dblack=\"berry \\\\backslashes\\\\ \" -Dcherry=berry" IGNORE_WHITESPACE
  test_parse_java_property "spark.$delimiter.12" \
    "-Dstraw=\"berry space\" -Dblue=\"berry \\\"quotes\\\"\" -Dblack=\"berry \\\\backslashes\\\\ \" -Dcherry=berry" IGNORE_WHITESPACE
done
echo

# Test split_java_options. Note that this relies on parse_java_property to work correctly.
if [[ "$tests_failed" == 1 ]]; then
  echo "* WARNING: Tests for parse_java_property failed!"
  echo -e "This should also fail tests for split_java_options\n"
fi
echo "--- Testing split_java_options ---"
test_split_java_options "spark.space.1" 1 "-Dstraw=berry"
test_split_java_options "spark.space.2" 1 "-Dstraw=berry"
test_split_java_options "spark.space.3" 1 "-Dstraw=berry again"
test_split_java_options "spark.space.4" 1 "-Dstraw=berry \"quote"
test_split_java_options "spark.space.5" 1 "-Dstraw=berry \\backslash"
test_split_java_options "spark.space.6" 1 "-Dstraw=berry \"quotes\" and \\backslashes\\ "
test_split_java_options "spark.space.7" 3 "-Dstraw=berry" "-Dblue=berry" "-Dblack=berry"
test_split_java_options "spark.space.8" 3 "-Dstraw=berry space" "-Dblue=berry" "-Dblack=berry"
test_split_java_options "spark.space.9" 3 \
  "-Dstraw=berry space" "-Dblue=berry \"quotes\"" "-Dblack=berry \\backslashes\\ "
test_split_java_options "spark.space.10" 3 \
  "-Dstraw=berry space" "-Dblue=berry \"quotes\"" "-Dblack=berry \\backslashes\\ "
test_split_java_options "spark.space.11" 4 \
  "-Dstraw=berry space" "-Dblue=berry \"quotes\"" "-Dblack=berry \\backslashes\\ " "-Dcherry=berry"
test_split_java_options "spark.space.12" 4 \
  "-Dstraw=berry space" "-Dblue=berry \"quotes\"" "-Dblack=berry \\backslashes\\ " "-Dcherry=berry"
echo

# Test quote_java_property. Note that this relies on split_java_options to work correctly.
if [[ "$tests_failed" == 1 ]]; then
  echo "* WARNING: Tests for split_java_options failed!"
  echo -e "This should also fail tests for quote_java_property\n"
fi
echo "--- Testing quote_java_property ---"
test_quote_java_property "spark.space.1" 1 "\"-Dstraw=berry\""
test_quote_java_property "spark.space.2" 1 "\"-Dstraw=berry\""
test_quote_java_property "spark.space.3" 1 "\"-Dstraw=berry again\""
test_quote_java_property "spark.space.4" 1 "\"-Dstraw=berry \"quote\""
test_quote_java_property "spark.space.5" 1 "\"-Dstraw=berry \\backslash\""
test_quote_java_property "spark.space.6" 1 "\"-Dstraw=berry \"quotes\" and \\backslashes\\ \""
test_quote_java_property "spark.space.7" 3 "\"-Dstraw=berry\"" "\"-Dblue=berry\"" "\"-Dblack=berry\""
test_quote_java_property "spark.space.8" 3 "\"-Dstraw=berry space\"" "\"-Dblue=berry\"" "\"-Dblack=berry\""
test_quote_java_property "spark.space.9" 3 \
  "\"-Dstraw=berry space\"" "\"-Dblue=berry \"quotes\"\"" "\"-Dblack=berry \\backslashes\\ \""
test_quote_java_property "spark.space.10" 3 \
  "\"-Dstraw=berry space\"" "\"-Dblue=berry \"quotes\"\"" "\"-Dblack=berry \\backslashes\\ \""
test_quote_java_property "spark.space.11" 4 \
  "\"-Dstraw=berry space\"" "\"-Dblue=berry \"quotes\"\"" "\"-Dblack=berry \\backslashes\\ \"" "\"-Dcherry=berry\""
test_quote_java_property "spark.space.12" 4 \
  "\"-Dstraw=berry space\"" "\"-Dblue=berry \"quotes\"\"" "\"-Dblack=berry \\backslashes\\ \"" "\"-Dcherry=berry\""
echo

# Final test result
if [[ "$tests_failed" == 0 ]]; then
  echo "**********************"
  echo "      TESTS PASS      "
  echo "**********************"
else 
  echo "XXXXXXXXXXXXXXXXXXXXXXXX"
  echo "XXXXX TESTS FAILED XXXXX"
  echo "XXXXXXXXXXXXXXXXXXXXXXXX"
  exit 1
fi

