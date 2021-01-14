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

def toJArray(gateway, jtype, arr):
    """
    Convert python list to java type array

    Parameters
    ----------
    gateway :
        Py4j Gateway
    jtype :
        java type of element in array
    arr :
        python type list
    """
    jarray = gateway.new_array(jtype, len(arr))
    for i in range(0, len(arr)):
        jarray[i] = arr[i]
    return jarray


def require_test_compiled():
    """ Raise Exception if test classes are not compiled
    """
    import os
    import glob
    try:
        spark_home = os.environ['SPARK_HOME']
    except KeyError:
        raise RuntimeError('SPARK_HOME is not defined in environment')

    test_class_path = os.path.join(
        spark_home, 'sql', 'core', 'target', '*', 'test-classes')
    paths = glob.glob(test_class_path)

    if len(paths) == 0:
        raise RuntimeError(
            "%s doesn't exist. Spark sql test classes are not compiled." % test_class_path)


class ForeachBatchFunction(object):
    """
    This is the Python implementation of Java interface 'ForeachBatchFunction'. This wraps
    the user-defined 'foreachBatch' function such that it can be called from the JVM when
    the query is active.
    """

    def __init__(self, sql_ctx, func):
        self.sql_ctx = sql_ctx
        self.func = func

    def call(self, jdf, batch_id):
        from pyspark.sql.dataframe import DataFrame
        try:
            self.func(DataFrame(jdf, self.sql_ctx), batch_id)
        except Exception as e:
            self.error = e
            raise e

    class Java:
        implements = ['org.apache.spark.sql.execution.streaming.sources.PythonForeachBatchFunction']


def to_str(value):
    """
    A wrapper over str(), but converts bool values to lower case strings.
    If None is given, just returns None, instead of converting it to string "None".
    """
    if isinstance(value, bool):
        return str(value).lower()
    elif value is None:
        return value
    else:
        return str(value)
