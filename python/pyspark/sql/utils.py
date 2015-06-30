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

import py4j


class AnalysisException(Exception):
    pass


def capture_sql_exception(f):
    def deco(*a, **kw):
        try:
            return f(*a, **kw)
        except py4j.protocol.Py4JJavaError, e:
            cls, msg = e.java_exception.toString().split(': ', 1)
            if cls == 'org.apache.spark.sql.AnalysisException':
                raise AnalysisException(msg)
            raise
    return deco


def install_exception_handler():
    old_func = py4j.protocol.get_return_value
    new_func = capture_sql_exception(old_func)
    # only patch the one used in in py4j.java_gateway (call Java API)
    py4j.java_gateway.get_return_value = new_func
