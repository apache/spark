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

from collections import namedtuple
import os
import traceback


CallSite = namedtuple("CallSite", "function file linenum")


def first_spark_call():
    """
    Return a CallSite representing the first Spark call in the current call stack.
    """
    tb = traceback.extract_stack()
    if len(tb) == 0:
        return None
    file, line, module, what = tb[len(tb) - 1]
    sparkpath = os.path.dirname(file)
    first_spark_frame = len(tb) - 1
    for i in range(0, len(tb)):
        file, line, fun, what = tb[i]
        if file.startswith(sparkpath):
            first_spark_frame = i
            break
    if first_spark_frame == 0:
        file, line, fun, what = tb[0]
        return CallSite(function=fun, file=file, linenum=line)
    sfile, sline, sfun, swhat = tb[first_spark_frame]
    ufile, uline, ufun, uwhat = tb[first_spark_frame - 1]
    return CallSite(function=sfun, file=ufile, linenum=uline)


class SCCallSiteSync:
    """
    Helper for setting the spark context call site.

    Example usage:
    from pyspark.context import SCCallSiteSync
    with SCCallSiteSync(<relevant SparkContext>) as css:
        <a Spark call>
    """

    _spark_stack_depth = 0

    def __init__(self, sc):
        call_site = first_spark_call()
        if call_site is not None:
            self._call_site = "%s at %s:%s" % (
                call_site.function,
                call_site.file,
                call_site.linenum,
            )
        else:
            self._call_site = "Error! Could not extract traceback info"
        self._context = sc

    def __enter__(self):
        if SCCallSiteSync._spark_stack_depth == 0:
            self._context._jsc.setCallSite(self._call_site)
        SCCallSiteSync._spark_stack_depth += 1

    def __exit__(self, type, value, tb):
        SCCallSiteSync._spark_stack_depth -= 1
        if SCCallSiteSync._spark_stack_depth == 0:
            self._context._jsc.setCallSite(None)
