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

from pyspark.resultiterable import ResultIterable

def _do_python_join(rdd, other, numPartitions, dispatch):
    vs = rdd.map(lambda (k, v): (k, (1, v)))
    ws = other.map(lambda (k, v): (k, (2, v)))
    return vs.union(ws).groupByKey(numPartitions).flatMapValues(lambda x : dispatch(x.__iter__()))


def python_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for (n, v) in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        return [(v, w) for v in vbuf for w in wbuf]
    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_right_outer_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for (n, v) in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        if not vbuf:
            vbuf.append(None)
        return [(v, w) for v in vbuf for w in wbuf]
    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_left_outer_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for (n, v) in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        if not wbuf:
            wbuf.append(None)
        return [(v, w) for v in vbuf for w in wbuf]
    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_cogroup(rdd, other, numPartitions):
    vs = rdd.map(lambda (k, v): (k, (1, v)))
    ws = other.map(lambda (k, v): (k, (2, v)))
    def dispatch(seq):
        vbuf, wbuf = [], []
        for (n, v) in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        return (ResultIterable(vbuf), ResultIterable(wbuf))
    return vs.union(ws).groupByKey(numPartitions).mapValues(dispatch)
