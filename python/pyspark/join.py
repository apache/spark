"""
Copyright (c) 2011, Douban Inc. <http://www.douban.com/>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.

    * Neither the name of the Douban Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from functools import reduce

from pyspark.resultiterable import ResultIterable


def _do_python_join(rdd, other, numPartitions, dispatch):
    vs = rdd.mapValues(lambda v: (1, v))
    ws = other.mapValues(lambda v: (2, v))
    return vs.union(ws).groupByKey(numPartitions).flatMapValues(lambda x: dispatch(x.__iter__()))


def python_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for n, v in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        return ((v, w) for v in vbuf for w in wbuf)

    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_right_outer_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for n, v in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        if not vbuf:
            vbuf.append(None)
        return ((v, w) for v in vbuf for w in wbuf)

    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_left_outer_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for n, v in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        if not wbuf:
            wbuf.append(None)
        return ((v, w) for v in vbuf for w in wbuf)

    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_full_outer_join(rdd, other, numPartitions):
    def dispatch(seq):
        vbuf, wbuf = [], []
        for n, v in seq:
            if n == 1:
                vbuf.append(v)
            elif n == 2:
                wbuf.append(v)
        if not vbuf:
            vbuf.append(None)
        if not wbuf:
            wbuf.append(None)
        return ((v, w) for v in vbuf for w in wbuf)

    return _do_python_join(rdd, other, numPartitions, dispatch)


def python_cogroup(rdds, numPartitions):
    def make_mapper(i):
        return lambda v: (i, v)

    vrdds = [rdd.mapValues(make_mapper(i)) for i, rdd in enumerate(rdds)]
    union_vrdds = reduce(lambda acc, other: acc.union(other), vrdds)
    rdd_len = len(vrdds)

    def dispatch(seq):
        bufs = [[] for _ in range(rdd_len)]
        for n, v in seq:
            bufs[n].append(v)
        return tuple(ResultIterable(vs) for vs in bufs)

    return union_vrdds.groupByKey(numPartitions).mapValues(dispatch)
