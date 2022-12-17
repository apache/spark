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


ERROR_CLASSES = {
    "NOT_COLUMN_OR_STRING": lambda arg_name, arg_type: f"Argument '{arg_name}' should be a column or string, got '{arg_type}'.",
    "NOT_COLUMN_OR_INTEGER_OR_STRING": lambda arg_name, arg_type: f"Argument '{arg_name}' should be a column or integer, got '{arg_type}'.",
    "NOT_A_COLUMN": lambda arg_name, arg_type: f"Argument '{arg_name}' should be a column, got '{arg_type}'.",
    "NOT_A_STRING": lambda arg_name, arg_type: f"Argument '{arg_name}' should be a string, got '{arg_type}'.",
    "COLUMN_IN_LIST": lambda func_name: f"{func_name} does not allow a column in a list",
    "WRONG_NUM_COLUMNS": lambda func_name: f"'{func_name}' should take at least two columns",
    "WRONG_NUM_ARGUMENTS": lambda func_name: f"'{func_name}' should take at least two columns",
    "WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION": lambda func_name, num_args: f"Function '{func_name}' should take between 1 and 3 arguments, but provided function takes {num_args}",
    "UNSUPPORTED_PARAMETER_FOR_HIGHER_ORDER_FUNCTION": lambda func_name: f"Function '{func_name}' should use only POSITIONAL or POSITIONAL OR KEYWORD arguments",
}
