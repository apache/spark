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

import os
import sys
import platform
import shutil
import warnings

from pyspark.serializers import BatchedSerializer, AutoSerializer

try:
    import psutil

    def get_used_memory():
        self = psutil.Process(os.getpid())
        return self.memory_info().rss >> 20

except ImportError:

    def get_used_memory():
        if platform.system() == 'Linux':
            for line in open('/proc/self/status'):
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) >> 10
        else:
            warnings.warn("please install psutil to get accurate memory usage")
            if platform.system() == "Darwin":
                import resource
                return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss >> 20
            # TODO: support windows
        return 0


class Merger(object):
    """
    merge shuffled data together by combinator
    """

    def merge(self, iterator):
        raise NotImplementedError

    def iteritems(self):
        raise NotImplementedError


class MapMerger(Merger):
    """
    In memory merger based on map
    """

    def __init__(self, combiner):
        self.combiner = combiner
        self.data = {}

    def merge(self, iterator):
        d, comb = self.data, self.combiner
        for k, v in iter(iterator):
            d[k] = comb(d[k], v) if k in d else v

    def iteritems(self):
        return self.data.iteritems()


class ExternalHashMapMerger(Merger):

    """
    External merger will dump the aggregated data into disks when memory usage
    is above the limit, then merge them together.

    >>> combiner = lambda x, y:x+y
    >>> merger = ExternalHashMapMerger(combiner, 10)
    >>> N = 10000
    >>> merger.merge(zip(xrange(N), xrange(N)) * 10)
    >>> assert merger.spills > 0
    >>> sum(v for k,v in merger.iteritems())
    499950000
    """

    PARTITIONS = 64
    BATCH = 10000

    def __init__(self, combiner, memory_limit=512, serializer=None,
            localdirs=None, scale=1):
        self.combiner = combiner
        self.memory_limit = memory_limit
        self.serializer = serializer or\
                BatchedSerializer(AutoSerializer(), 1024)
        self.localdirs = localdirs or self._get_dirs()
        self.scale = scale
        self.data = {}
        self.pdata = []
        self.spills = 0

    def _get_dirs(self):
        path = os.environ.get("SPARK_LOCAL_DIR", "/tmp/spark")
        dirs = path.split(",")
        localdirs = []
        for d in dirs:
            d = os.path.join(d, "merge", str(os.getpid()))
            try:
                os.makedirs(d)
                localdirs.append(d)
            except IOError:
                pass
        if not localdirs:
            raise IOError("no writable directories: " + path)
        return localdirs

    def _get_spill_dir(self, n):
        return os.path.join(self.localdirs[n % len(self.localdirs)], str(n))

    @property
    def used_memory(self):
        return get_used_memory()

    @property
    def next_limit(self):
        return max(self.memory_limit, self.used_memory * 1.05)

    def merge(self, iterator, check=True):
        """ merge (K,V) pair by combiner """
        iterator = iter(iterator)
        # speedup attribute lookup
        d, comb, batch = self.data, self.combiner, self.BATCH
        c = 0
        for k, v in iterator:
            d[k] = comb(d[k], v) if k in d else v
            if not check:
                continue

            c += 1
            if c % batch == 0 and self.used_memory > self.memory_limit:
                self._first_spill()
                self._partitioned_merge(iterator, self.next_limit)
                break

    def _hash(self, key):
        return (hash(key) / self.scale) % self.PARTITIONS

    def _partitioned_merge(self, iterator, limit):
        comb, pdata, hfun = self.combiner, self.pdata, self._hash
        c = 0
        for k, v in iterator:
            d = pdata[hfun(k)]
            d[k] = comb(d[k], v) if k in d else v
            if not limit:
                continue
            c += 1
            if c % self.BATCH == 0 and self.used_memory > limit:
                self._spill()
                limit = self.next_limit

    def _first_spill(self):
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)
        streams = [open(os.path.join(path, str(i)), 'w')
                   for i in range(self.PARTITIONS)]
        for k, v in self.data.iteritems():
            h = self._hash(k)
            self.serializer.dump_stream([(k, v)], streams[h])
        for s in streams:
            s.close()
        self.data.clear()
        self.pdata = [{} for i in range(self.PARTITIONS)]
        self.spills += 1

    def _spill(self):
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)
        for i in range(self.PARTITIONS):
            p = os.path.join(path, str(i))
            with open(p, "w") as f:
                self.serializer.dump_stream(self.pdata[i].iteritems(), f)
            self.pdata[i].clear()
        self.spills += 1

    def iteritems(self):
        """ iterator of all merged (K,V) pairs """
        if not self.pdata and not self.spills:
            return self.data.iteritems()
        return self._external_items()

    def _external_items(self):
        assert not self.data
        if any(self.pdata):
            self._spill()
        hard_limit = self.next_limit

        try:
            for i in range(self.PARTITIONS):
                self.data = {}
                for j in range(self.spills):
                    path = self._get_spill_dir(j)
                    p = os.path.join(path, str(i))
                    self.merge(self.serializer.load_stream(open(p)), False)

                    if self.used_memory > hard_limit and j < self.spills - 1:
                        self.data.clear() # will read from disk again
                        for v in self._recursive_merged_items(i):
                            yield v
                        return

                for v in self.data.iteritems():
                    yield v
                self.data.clear()
        finally:
            self._cleanup()

    def _cleanup(self):
        for d in self.localdirs:
            shutil.rmtree(d, True)

    def _recursive_merged_items(self, start):
        assert not self.data
        assert self.spills > 0
        if any(self.pdata):
            self._spill()

        for i in range(start, self.PARTITIONS):
            subdirs = [os.path.join(d, "merge", str(i))
                            for d in self.localdirs]
            m = ExternalHashMapMerger(self.combiner, self.memory_limit,
                    self.serializer, subdirs, self.scale * self.PARTITIONS)
            m.pdata = [{} for _ in range(self.PARTITIONS)]
            limit = self.next_limit

            for j in range(self.spills):
                path = self._get_spill_dir(j)
                p = os.path.join(path, str(i))
                m._partitioned_merge(self.serializer.load_stream(open(p)), 0)
                if m.used_memory > limit:
                    m._spill()
                    limit = self.next_limit

            for v in m._external_items():
                yield v


if __name__ == "__main__":
    import doctest
    doctest.testmod()
